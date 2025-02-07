;;; jupyter-kernel-manager.el --- Jupyter kernel manager -*- lexical-binding: t -*-

;; Copyright (C) 2018 Nathaniel Nicandro

;; Author: Nathaniel Nicandro <nathanielnicandro@gmail.com>
;; Created: 08 Jan 2018
;; Version: 0.8.0

;; This program is free software; you can redistribute it and/or
;; modify it under the terms of the GNU General Public License as
;; published by the Free Software Foundation; either version 3, or (at
;; your option) any later version.

;; This program is distributed in the hope that it will be useful, but
;; WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
;; General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with GNU Emacs; see the file COPYING.  If not, write to the
;; Free Software Foundation, Inc., 59 Temple Place - Suite 330,
;; Boston, MA 02111-1307, USA.

;;; Commentary:

;; Manage a local Jupyter kernel process.

;;; Code:

(require 'jupyter-base)
(require 'jupyter-messages)
(require 'jupyter-client)
(eval-when-compile (require 'subr-x))

(declare-function ansi-color-apply "ansi-color" (string))

(defgroup jupyter-kernel-manager nil
  "Jupyter kernel manager"
  :group 'jupyter)

;;; `jupyter-kernel-process'

(defclass jupyter-kernel-lifetime (jupyter-finalized-object)
  ()
  :abstract t
  :documentation "Trait to control the lifetime of a kernel.")

(cl-defmethod initialize-instance ((kernel jupyter-kernel-lifetime) &optional _slots)
  (cl-call-next-method)
  (jupyter-add-finalizer kernel
    (lambda ()
      (when (jupyter-kernel-alive-p kernel)
        (jupyter-kill-kernel kernel)))))

(cl-defgeneric jupyter-kernel-alive-p ((kernel jupyter-kernel-lifetime))
  "Return non-nil if KERNEL is alive.")

(cl-defgeneric jupyter-start-kernel ((kernel jupyter-kernel-lifetime) &rest args)
  "Start KERNEL.")

(cl-defgeneric jupyter-kill-kernel ((kernel jupyter-kernel-lifetime))
  "Tell the KERNEL to stop.")

(cl-defgeneric jupyter-kernel-died ((kernel jupyter-kernel-lifetime))
  "Called when a KERNEL dies unexpectedly.")

(cl-defmethod jupyter-start-kernel :around ((kernel jupyter-kernel-lifetime) &rest _args)
  "Error when KERNEL is already alive, otherwise call the next method."
  (when (jupyter-kernel-alive-p kernel)
    (error "Kernel already alive"))
  (cl-call-next-method))

(cl-defmethod jupyter-kill-kernel :around ((kernel jupyter-kernel-lifetime))
  "Call the next method only when KERNEL is alive."
  (when (jupyter-kernel-alive-p kernel)
    (cl-call-next-method)))

(cl-defmethod jupyter-kernel-died ((_kernel jupyter-kernel-lifetime))
  (ignore))

(defclass jupyter-meta-kernel (jupyter-kernel-lifetime)
  ((spec
    :type cons
    :initarg :spec
    :documentation "The kernelspec for this kernel.
SPEC is in the same format as one of the elements returned by
`jupyter-find-kernelspecs'.")
   (session
    :type jupyter-session
    :initarg :session
    :documentation "The session used for communicating with the kernel.
This slot is set to an unbound state after a call to
`jupyter-kill-kernel'."))
  :abstract t
  :documentation "Partial representation of a Jupyter kernel.

Contains the kernelspec associated with the kernel and the
`jupyter-session' object used for communicating with the kernel
when it is alive.

Sub-classes must call `cl-next-method-method' in their
implementation of `jupyter-kill-kernel'.

A convenience method, `jupyter-kernel-name', is provided to
access the name of the kernelspec.")

(cl-defmethod jupyter-kill-kernel ((kernel jupyter-meta-kernel))
  (when (slot-boundp kernel 'session)
    (slot-makeunbound kernel 'session)))

(cl-defmethod jupyter-kernel-name ((kernel jupyter-meta-kernel))
  "Return the name of KERNEL."
  (car (oref kernel spec)))

(defclass jupyter-kernel-process (jupyter-meta-kernel)
  ((process
    :type process
    :documentation "The kernel process."))
  :documentation "A Jupyter kernel process.
Starts a kernel process using `start-file-process'.

If the kernel was started on a remote host, ensure that local
tunnels are created when setting the session slot after the
kernel starts.")

(cl-defmethod jupyter-kernel-alive-p ((kernel jupyter-kernel-process))
  (and (slot-boundp kernel 'process)
       (process-live-p (oref kernel process))))

(cl-defmethod jupyter-start-kernel ((kernel jupyter-kernel-process) &rest args)
  "Start a KERNEL process with ARGS."
  (let ((name (jupyter-kernel-name kernel)))
    (when jupyter--debug
      (message "jupyter-start-kernel: default-directory = %s" default-directory)
      (message "jupyter-start-kernel: Starting process with args \"%s\""
               (mapconcat #'identity args " ")))
    (oset kernel process
          (apply #'start-file-process
                 (format "jupyter-kernel-%s" name)
                 (generate-new-buffer
                  (format " *jupyter-kernel[%s]*" name))
                 (car args) (cdr args)))
    (set-process-query-on-exit-flag
     (oref kernel process) jupyter--debug)))

(defun jupyter--kernel-died-process-sentinel (kernel)
  "Return a sentinel function calling KERNEL's `jupyter-kernel-died' method.
The method will be called when the process exits or receives a
fatal signal."
  (cl-check-type kernel jupyter-kernel-lifetime)
  (let ((ref (jupyter-weak-ref kernel)))
    (lambda (process _)
      (when (memq (process-status process) '(exit signal))
        (when-let* ((kernel (jupyter-weak-ref-resolve ref)))
          (jupyter-kernel-died kernel))))))

(cl-defmethod jupyter-start-kernel :after ((kernel jupyter-kernel-process) &rest _args)
  (setf (process-sentinel (oref kernel process))
        (jupyter--kernel-died-process-sentinel kernel)))

(cl-defmethod jupyter-kill-kernel ((kernel jupyter-kernel-process))
  (with-slots (process) kernel
    (delete-process process)
    (when (buffer-live-p (process-buffer process))
      (kill-buffer (process-buffer process))))
  (cl-call-next-method))

(defclass jupyter-command-kernel (jupyter-kernel-process)
  ()
  :documentation "A Jupyter kernel process using the \"jupyter kernel\" command.")

(cl-defmethod jupyter-start-kernel ((kernel jupyter-command-kernel) &rest args)
  "Start KERNEL, passing ARGS as command line arguments to \"jupyter kernel\".
The --kernel argument of \"jupyter kernel\" is filled in with the
`jupyter-kernel-name' of KERNEL and passed as the first
argument of the process."
  ;; NOTE: On Windows, apparently the "jupyter kernel" command uses something
  ;; like an exec shell command to start the process which launches the kernel,
  ;; but exec like commands on Windows start a new process instead of replacing
  ;; the current one which results in the process we start here exiting after
  ;; the new process is launched. We call python directly to avoid this.
  (apply #'cl-call-next-method
         kernel (jupyter-locate-python)
         "-c" "from jupyter_client.kernelapp import main; main()"
         (format "--kernel=%s" (jupyter-kernel-name kernel))
         args))

(cl-defmethod jupyter-start-kernel :after ((kernel jupyter-command-kernel) &rest _args)
  "Set the session slot from KERNEL's process output."
  (with-slots (process) kernel
    (with-current-buffer (process-buffer process)
      (jupyter-with-timeout
          ((format "Launching %s kernel process..." (jupyter-kernel-name kernel))
           jupyter-long-timeout
           (if (process-live-p process)
               (error "\
`jupyter kernel` output did not show connection file within timeout")
             (error "Kernel process exited:\n%s"
                    (ansi-color-apply (buffer-string)))))
        (and (process-live-p process)
             (goto-char (point-min))
             (re-search-forward "Connection file: \\(.+\\)\n" nil t)))
      (let* ((conn-file (match-string 1))
             (remote (file-remote-p default-directory))
             (conn-info (if remote (jupyter-tunnel-connection
                                    (concat remote conn-file))
                          (jupyter-read-plist conn-file))))
        (oset kernel session (jupyter-session
                              :conn-info conn-info
                              :key (plist-get conn-info :key)))))))

(defclass jupyter-spec-kernel (jupyter-kernel-process)
  ()
  :documentation "A Jupyter kernel launched from a kernelspec.")

(defun jupyter--block-until-conn-file-access (atime kernel conn-file)
  (with-slots (process) kernel
    (jupyter-with-timeout
        ((format "Starting %s kernel process..." (jupyter-kernel-name kernel))
         jupyter-long-timeout
         ;; If the process is still alive, punt farther down the line.
         (unless (process-live-p process)
           (error "Kernel process exited:\n%s"
                  (with-current-buffer (process-buffer process)
                    (ansi-color-apply (buffer-string))))))
      (let ((attribs (file-attributes conn-file)))
        ;; `file-attributes' can potentially return nil, in this case
        ;; just assume it has read the connection file so that we can
        ;; know for sure it is not connected if it fails to respond to
        ;; any messages we send it.
        (or (null attribs)
            (not (equal atime (nth 4 attribs))))))))

(cl-defmethod jupyter-start-kernel ((kernel jupyter-spec-kernel) &rest _args)
  (cl-destructuring-bind (_name . (resource-dir . spec)) (oref kernel spec)
    (let ((conn-file (jupyter-write-connection-file
                      (oref kernel session) kernel))
          (process-environment
           (append
            ;; The first entry takes precedence when duplicated
            ;; variables are found in `process-environment'
            (cl-loop
             for (k v) on (plist-get spec :env) by #'cddr
             collect (format "%s=%s" (cl-subseq (symbol-name k) 1) v))
            process-environment)))
      (let ((atime (nth 4 (file-attributes conn-file))))
        (apply #'cl-call-next-method
               kernel (cl-loop
                       for arg in (append (plist-get spec :argv) nil)
                       if (equal arg "{connection_file}")
                       collect (file-local-name conn-file)
                       else if (equal arg "{resource_dir}")
                       collect (file-local-name resource-dir)
                       else collect arg))
        ;; Windows systems may not have good time resolution when retrieving
        ;; the last access time of a file so we don't bother with checking that
        ;; the kernel has read the connection file and leave it to the
        ;; downstream initialization to ensure that we can communicate with a
        ;; kernel.
        (unless (memq system-type '(ms-dos windows-nt cygwin))
          (jupyter--block-until-conn-file-access atime kernel conn-file))))))

(defclass jupyter-kernel-manager (jupyter-kernel-lifetime)
  ((kernel
    :type jupyter-meta-kernel
    :initarg :kernel
    :documentation "The name of the kernel that is being managed.")
   (control-channel
    :type (or null jupyter-sync-channel)
    :initform nil
    :documentation "The kernel's control channel.")))

(cl-defgeneric jupyter-make-client ((manager jupyter-kernel-manager) class &rest slots)
  "Make a new client from CLASS connected to MANAGER's kernel.
SLOTS are the slots used to initialize the client with.")

(cl-defmethod jupyter-make-client :before (_manager class &rest _slots)
  "Signal an error if CLASS is not a subclass of `jupyter-kernel-client'."
  (unless (child-of-class-p class 'jupyter-kernel-client)
    (signal 'wrong-type-argument (list '(subclass jupyter-kernel-client) class))))

(cl-defmethod jupyter-make-client (manager class &rest slots)
  "Return an instance of CLASS using SLOTS and its manager slot set to MANAGER."
  (let ((client (apply #'make-instance class slots)))
    (prog1 client
      (oset client manager manager))))

(cl-defmethod jupyter-make-client ((manager jupyter-kernel-manager) _class &rest _slots)
  "Make a new client from CLASS connected to MANAGER's kernel.
CLASS should be a subclass of `jupyter-kernel-client', a new
instance of CLASS is initialized with SLOTS and configured to
connect to MANAGER's kernel."
  (let ((client (cl-call-next-method)))
    (with-slots (kernel) manager
      (prog1 client
        ;; TODO: We can also have the manager hold the kcomm object and just
        ;; pass a single kcomm object to all clients using this manager since the
        ;; kcomm broadcasts event to all connected clients. This is more
        ;; efficient as it only uses one subprocess for every client connected to
        ;; a kernel.
        (oset client kcomm (jupyter-channel-ioloop-comm))
        (jupyter-initialize-connection client (oref kernel session))))))

(cl-defmethod jupyter-start-kernel ((manager jupyter-kernel-manager) &rest args)
  "Start MANAGER's kernel."
  (unless (jupyter-kernel-alive-p manager)
    (with-slots (kernel) manager
      (apply #'jupyter-start-kernel kernel args)
      (jupyter-start-channels manager))))

(cl-defmethod jupyter-start-kernel :after ((manager jupyter-kernel-manager) &rest _args)
  (with-slots (kernel) manager
    (when (object-of-class-p kernel 'jupyter-kernel-process)
      (add-function
       :after (process-sentinel (oref kernel process))
       (jupyter--kernel-died-process-sentinel manager)))))

(cl-defmethod jupyter-start-channels ((manager jupyter-kernel-manager))
  "Start a control channel on MANAGER."
  (with-slots (kernel control-channel) manager
    (if control-channel (jupyter-start-channel control-channel)
      (cl-destructuring-bind (&key transport ip control_port &allow-other-keys)
          (jupyter-session-conn-info (oref kernel session))
        (oset manager control-channel
              (jupyter-sync-channel
               :type :control
               :session (oref kernel session)
               :endpoint (format "%s://%s:%d" transport ip control_port)))
        (jupyter-start-channels manager)))))

(cl-defmethod jupyter-stop-channels ((manager jupyter-kernel-manager))
  "Stop the control channel on MANAGER."
  (when-let* ((channel (oref manager control-channel)))
    (jupyter-stop-channel channel)
    (oset manager control-channel nil)))

(cl-defgeneric jupyter-shutdown-kernel ((manager jupyter-kernel-manager) &optional restart timeout)
  "Shutdown MANAGER's kernel or restart instead if RESTART is non-nil.
Wait until TIMEOUT before forcibly shutting down the kernel.")

(cl-defmethod jupyter-kill-kernel ((manager jupyter-kernel-manager))
  (jupyter-shutdown-kernel manager))

(cl-defmethod jupyter-shutdown-kernel ((manager jupyter-kernel-manager) &optional restart timeout)
  "Shutdown MANAGER's kernel with an optional RESTART.
If RESTART is non-nil, then restart the kernel after shutdown.
First send a shutdown request on the control channel to the
kernel. If the kernel has not shutdown within TIMEOUT, forcibly
kill the kernel subprocess. After shutdown the MANAGER's control
channel is stopped unless RESTART is non-nil."
  (when (jupyter-kernel-alive-p manager)
    ;; FIXME: For some reason the control-channel is nil sometimes
    (jupyter-start-channels manager)
    (with-slots (control-channel kernel) manager
      (jupyter-send control-channel :shutdown-request
                    (jupyter-message-shutdown-request :restart restart))
      ;; FIXME: This doesn't work properly, the kernel sends a shutdown reply
      ;; but the process status cannot be determined correctly as it is still
      ;; considered alive. This is mainly when using the
      ;; `jupyter-command-kernel' and probably has to do with the fact that the
      ;; kernel is launched by a python process instead of being launched
      ;; directly as a process by Emacs.
      (jupyter-with-timeout
          ((format "%s kernel shutting down..."
                   (jupyter-kernel-name kernel))
           (or timeout jupyter-default-timeout)
           (message "%s kernel did not shutdown by request"
                    (jupyter-kernel-name kernel))
           (jupyter-kill-kernel kernel))
        (not (jupyter-kernel-alive-p manager)))
      (if restart
          (jupyter-start-kernel manager)
        (jupyter-stop-channels manager)))))

(cl-defgeneric jupyter-interrupt-kernel ((manager jupyter-kernel-manager) &optional timeout)
  "Interrupt MANAGER's kernel.
When the kernel has an interrupt mode of \"message\" send an
interrupt request and wait until TIMEOUT for a reply.")

(cl-defmethod jupyter-interrupt-kernel ((manager jupyter-kernel-manager) &optional timeout)
  "Interrupt MANAGER's kernel.
If the kernel's interrupt mode is set to \"message\" send an
interrupt request on MANAGER's control channel and wait until
TIMEOUT for a reply. Otherwise if the kernel does not specify an
interrupt mode, send an interrupt signal to the kernel
subprocess."
  (when (jupyter-kernel-alive-p manager)
    ;; FIXME: For some reason the control-channel is nil sometimes
    (jupyter-start-channels manager)
    (with-slots (kernel) manager
      (cl-destructuring-bind (_name _resource-dir . spec) (oref kernel spec)
        (pcase (plist-get spec :interrupt_mode)
          ("message"
           (with-slots (control-channel) manager
             (jupyter-send control-channel :interrupt-request
                           (jupyter-message-interrupt-request))
             (jupyter-with-timeout
                 ((format "Interruptin %s kernel"
                          (jupyter-kernel-name kernel))
                  (or timeout jupyter-default-timeout)
                  (message "No interrupt reply from kernel (%s)"
                           (jupyter-kernel-name kernel)))
               (condition-case nil
                   (with-slots (session socket) control-channel
                     (jupyter-recv session socket zmq-DONTWAIT))
                 (zmq-EAGAIN nil)))))
          (_
           (if (object-of-class-p kernel 'jupyter-kernel-process)
               (interrupt-process (oref kernel process) t)
             (warn "Can't interrupt kernel"))))))))

(cl-defmethod jupyter-kernel-alive-p ((manager jupyter-kernel-manager))
  "Is MANGER's kernel alive?"
  (and (slot-boundp manager 'kernel)
       (jupyter-kernel-alive-p (oref manager kernel))))

(defun jupyter--error-if-no-kernel-info (client)
  (jupyter-kernel-info client))

(defun jupyter-start-new-kernel (kernel-name &optional client-class)
  "Start a managed Jupyter kernel.
KERNEL-NAME is the name of the kernel to start. It can also be
the prefix of a valid kernel name, in which case the first kernel
in `jupyter-available-kernelspecs' that has KERNEL-NAME as a
prefix will be used. Optional argument CLIENT-CLASS is a subclass
of `jupyer-kernel-client' and will be used to initialize a new
client connected to the kernel. CLIENT-CLASS defaults to the
symbol `jupyter-kernel-client'.

Return a list (KM KC) where KM is the kernel manager managing the
lifetime of the kernel subprocess. KC is a new client connected
to the kernel whose class is CLIENT-CLASS. The client is
connected to the kernel with all channels listening for messages
and the heartbeat channel unpaused. Note that the client's
`manager' slot will also be set to the kernel manager instance,
see `jupyter-make-client'.

Note, if `default-directory' is a remote directory, a kernel will
start on the remote host by using the \"jupyter kernel\" shell
command on the host."
  (or client-class (setq client-class 'jupyter-kernel-client))
  ;; TODO: Replace with
  ;; (cl-assert (child-of-class-p client-class 'jupyter-kernel-client))
  (jupyter-error-if-not-client-class-p client-class)
  (let* ((spec (jupyter-guess-kernelspec kernel-name))
         (kernel (if (file-remote-p default-directory)
                     (jupyter-command-kernel :spec spec)
                   (let* ((key (jupyter-new-uuid))
                          (conn-info (jupyter-create-connection-info
                                      :kernel-name kernel-name
                                      :key key)))
                     (jupyter-spec-kernel
                      :spec spec
                      ;; TODO: Convert `jupyter-session' into an object and
                      ;; only require `conn-info'.
                      :session (jupyter-session
                                :key key
                                :conn-info conn-info)))))
         (manager (jupyter-kernel-manager :kernel kernel)))
    (jupyter-start-kernel manager)
    (let ((client (jupyter-make-client manager client-class)))
      (jupyter-start-channels client)
      (jupyter--error-if-no-kernel-info client)
      ;; Un-pause the hearbeat after the kernel starts since waiting for
      ;; it to start may cause the heartbeat to think the kernel died.
      (jupyter-hb-unpause client)
      (list manager client))))

(provide 'jupyter-kernel-manager)

;;; jupyter-kernel-manager.el ends here
