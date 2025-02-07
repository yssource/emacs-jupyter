;;; jupyter-ioloop.el --- Jupyter channel subprocess -*- lexical-binding: t -*-

;; Copyright (C) 2018 Nathaniel Nicandro

;; Author: Nathaniel Nicandro <nathanielnicandro@gmail.com>
;; Created: 03 Nov 2018
;; Version: 0.8.0
;; Package-Requires: ((emacs "26"))

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

;; An ioloop encapsulates a subprocess that communicates with its parent
;; process in a pre-defined way. The parent process sends events (lists with a
;; head element tagging the type of event and the rest of the elements being
;; the arguments), via a call to the `jupyter-send' method of a
;; `jupyter-ioloop'. The ioloop subprocess then handles the event in its
;; environment. You add an event that can be handled in the ioloop environment
;; by calling `jupyter-ioloop-add-event' before calling `jupyter-ioloop-start'.
;;
;; In the event handler of the ioloop, you may optionally return another event
;; back to the parent process. In this case, when the parent process receives
;; the event it is dispatched to an appropriate `jupyter-ioloop-handler'.
;;
;; An example that will echo back what was sent to the ioloop as a message in
;; the parent process:
;;
;; (cl-defmethod jupyter-ioloop-handler ((ioloop jupyter-ioloop) (tag (eql :tag1)) (event (head echo)))
;;   (message "%s" (cadr event)))
;;
;; (let ((ioloop (jupyter-ioloop))
;;   (jupyter-ioloop-add-event ioloop echo (data)
;;     "Return DATA back to the parent process."
;;     (list 'echo data))
;;   (jupyter-ioloop-start ioloop :tag1)
;;   (jupyter-send ioloop 'echo "Message")
;;   (jupyter-ioloop-stop ioloop))

;;; Code:

(require 'jupyter-base)
(eval-when-compile (require 'subr-x))

(defvar jupyter-ioloop-poller nil
  "The polling object being used to poll for events in an ioloop.")

(defvar jupyter-ioloop-stdin nil
  "A file descriptor or ZMQ socket used to receive events in an ioloop.")

(defvar jupyter-ioloop-nsockets 1
  "The number of sockets being polled by `jupyter-ioloop-poller'.")

(defvar jupyter-ioloop-pre-hook nil
  "A hook called at the start of every polling loop.
The hook is called with no arguments.")

(defvar jupyter-ioloop-post-hook nil
  "A hook called at the end of every polling loop.
The hook is called with a single argument, the list of polling
events that occurred for this iteration, see
the return value of `zmq-poller-wait-all'.")

(defvar jupyter-ioloop-timers nil)

(defvar jupyter-ioloop-timeout 200)

(defvar jupyter-ioloop--argument-types nil
  "Argument types added via `jupyter-ioloop-add-arg-type'.")

(defclass jupyter-ioloop (jupyter-finalized-object)
  ((process :type (or null process) :initform nil)
   (callbacks :type list :initform nil)
   (events :type list :initform nil)
   (setup :type list :initform nil)
   (teardown :type list :initform nil))
  :documentation "An interface for sending asynchronous messages via a subprocess.

An ioloop starts an Emacs subprocess setup to send events back
and forth between the parent Emacs process and the ioloop
asynchronously. The ioloop subprocess is essentially a polling
loop that polls its stdin and any sockets that may have been
created in the ioloop environment and performs pre-defined
actions when stdin sends an event. The structure of the
subprocess is the following

\(progn
  (let ((jupyter-ioloop-poller (zmq-poller)))
    <jupyter-ioloop-setup>
    <send start event to parent>
    (condition-case nil
      (while t
        (run-hook 'jupyter-ioloop-pre-hook)
        <poll for stdin/socket events>
        (run-hook 'jupyter-ioloop-post-hook))
     (quit
       <jupyter-ioloop-teardown>
       <send quit event to parent>))))

<jupyter-ioloop-setup> is replaced by the form in the setup slot
of an ioloop and can be conveniently added to using
`jupyter-ioloop-add-setup'.

<jupyter-ioloop-teardown> is replaced with the teardown slot and
can be added to using `jupyter-ioloop-add-teardown'.

<poll for stdin/socket events> is replaced by code that will
listen for stdin/socket events using `jupyter-ioloop-poller'.

You add events to be handled by the subprocess using
`jupyter-ioloop-add-event', the return value of any event added
is what is sent to the parent Emacs process and what will
eventually be used as the EVENT argument of
`jupyter-ioloop-handler', which see. To suppress the subprocess
from sending anything back to the parent, ensure nil is returned
by the form created by `jupyter-ioloop-add-event'.

See `jupyter-channel-ioloop' for an example of its usage.")

(cl-defmethod initialize-instance ((ioloop jupyter-ioloop) &optional _slots)
  (cl-call-next-method)
  (jupyter-add-finalizer ioloop
    (lambda ()
      (with-slots (process) ioloop
        (when (process-live-p process)
          (delete-process process))))))

(cl-defgeneric jupyter-ioloop-handler ((_ioloop jupyter-ioloop) obj event)
  "Define a new IOLOOP handler, dispatching on OBJ, for EVENT.
OBJ will be the value of the object passed to
`jupyter-ioloop-start' and EVENT will be an event as received by
a filter function described in `zmq-start-process'."
  ;; Don't error on built in events
  (unless (memq (car-safe event) '(start quit))
    (error "Unhandled event (%s %s)" (type-of obj) event)))

(defun jupyter-ioloop-wait-until (ioloop event cb &optional timeout progress-msg)
  "Wait until EVENT occurs on IOLOOP.
If EVENT occurs, call CB and return its value if non-nil. CB is
called with a single argument, an event list whose first element
is EVENT. If CB returns nil, continue waiting until EVENT occurs
again or until TIMEOUT seconds elapses, TIMEOUT defaults to
`jupyter-default-timeout'. If TIMEOUT is reached, return nil.

If PROGRESS-MSG is non-nil, a progress reporter will be displayed
while waiting using PROGRESS-MSG as the message."
  (declare (indent 2))
  (cl-check-type ioloop jupyter-ioloop)
  (jupyter-with-timeout
      (progress-msg (or timeout jupyter-default-timeout))
    (let ((e (jupyter-ioloop-last-event ioloop)))
      (when (eq (car-safe e) event) (funcall cb e)))))

(defun jupyter-ioloop-last-event (ioloop)
  "Return the last event received on IOLOOP."
  (cl-check-type ioloop jupyter-ioloop)
  (and (oref ioloop process)
       (process-get (oref ioloop process) :last-event)))

(cl-defmethod jupyter-ioloop-handler :before ((ioloop jupyter-ioloop) _obj event)
  "Set the :last-event property of IOLOOP's process.
Additionally set the :start and :quit properties of the process
to t when they occur. See also `jupyter-ioloop-wait-until'."
  (with-slots (process) ioloop
    (cond
     ((eq (car-safe event) 'start)
      (process-put process :start t))
     ((eq (car-safe event) 'quit)
      (process-put process :quit t)))
    (process-put process :last-event event)))

(defmacro jupyter-ioloop-add-setup (ioloop &rest body)
  "Set IOLOOP's `jupyter-ioloop-setup' slot to BODY.
BODY is the code that will be evaluated before the IOLOOP sends a
start event to the parent process."
  (declare (indent 1))
  `(setf (oref ,ioloop setup)
         (append (oref ,ioloop setup)
                 (quote ,body))))

(defmacro jupyter-ioloop-add-teardown (ioloop &rest body)
  "Set IOLOOP's `jupyter-ioloop-teardown' slot to BODY.
BODY is the code that will be evaluated just before the IOLOOP
sends a quit event to the parent process.

After BODY is evaluated in the IOLOOP environment, the channels
in `jupyter-ioloop-channels' will be stopped before sending the
quit event."
  (declare (indent 1))
  `(setf (oref ,ioloop teardown)
         (append (oref ,ioloop teardown)
                 (quote ,body))))

(defmacro jupyter-ioloop-add-arg-type (tag fun)
  "Add a new argument type for arguments in `jupyter-ioloop-add-event'.
If an argument has the form (arg TAG), where TAG is a symbol, in
the ARGS argument of `jupyter-ioloop-add-event', replace it with
the result of evaluating the form returned by FUN on arg in the
IOLOOP environment.

For example suppose we define an argument type, jupyter-channel:

    (jupyter-ioloop-add-arg-type jupyter-channel
      (lambda (arg)
        `(or (object-assoc ,arg :type jupyter-ioloop-channels)
             (error \"Channel not alive (%s)\" ,arg))))

and define an event like

    (jupyter-ioloop-add-event ioloop stop-channel ((channel jupyter-channel))
      (jupyter-stop-channel channel))

Finally after adding other events and starting the ioloop we send
an event like

    (jupyter-send ioloop 'stop-channel :shell)

Then before the stop-channel event defined by
`jupyter-ioloop-add-event' is called in the IOLOOP environment,
the value for the channel argument passed by the `jupyter-send'
call is replaced by the form returned by the function specified
in the `jupyter-ioloop-add-arg-type' call."
  (declare (indent 1))
  `(progn
     (setq jupyter-ioloop--argument-types
           (delq (assoc ',tag jupyter-ioloop--argument-types)
                 jupyter-ioloop--argument-types))

     (push
      (cons ',tag
            ;; Ensure we don't create lexical closures
            ,(list '\` fun))
      jupyter-ioloop--argument-types)))

(defun jupyter-ioloop--replace-args (args)
  "Convert special arguments in ARGS.
Map over ARGS, converting its elements into

    ,arg or ,(app (lambda (x) BODY) arg)

for use in a `pcase' form. The latter form occurs when one of
ARGS is of the form (arg TAG) where TAG is one of the keys in
`jupyter-ioloop--argument-types'. BODY will be replaced with the
result of calling the function associated with TAG in
`jupyter-ioloop--argument-types'.

Return the list of converted arguments."
  (cl-loop
   with arg-type = nil
   for arg in args
   if (and (listp arg)
           (setq arg-type (assoc (cadr arg) jupyter-ioloop--argument-types)))
   ;; ,(app (lambda (x) ...) arg)
   collect (list '\, (list 'app `(lambda (x) ,(funcall (cdr arg-type) 'x))
                           (car arg)))
   ;; ,arg
   else collect (list '\, arg)))

(defmacro jupyter-ioloop-add-event (ioloop event args &optional doc &rest body)
  "For IOLOOP, add an EVENT handler.
ARGS is a list of arguments that are bound when EVENT occurs. DOC
is an optional documentation string describing what BODY, the
expression which will be evaluated when EVENT occurs, does. If
BODY evaluates to any non-nil value, it will be sent to the
parent Emacs process. A nil value for BODY means don't send
anything.

Some arguments are treated specially:

If one of ARGS is a list (<sym> tag) where <sym> is any symbol,
then the parent process that sends EVENT to IOLOOP is expected to
send a value that will be bound to <sym> and be handled by an
argument handler associated with tag before BODY is evaluated in
the IOLOOP process, see `jupyter-ioloop-add-arg-type'."
  (declare (indent 3) (doc-string 4) (debug t))
  (unless (stringp doc)
    (when doc
      (setq body (cons doc body))))
  `(setf (oref ,ioloop events)
         (cons (list (quote ,event) (quote ,args) (quote ,body))
               (cl-remove-if (lambda (x) (eq (car x) (quote ,event)))
                             (oref ,ioloop events)))))

(defun jupyter-ioloop--event-dispatcher (ioloop exp)
  "For IOLOOP return a form suitable for matching against EXP.
That is, return an expression which will cause an event to be
fired if EXP matches any event types handled by IOLOOP.

TODO: Explain these
By default this adds the events quit, callback, and timer."
  `(let* ((cmd ,exp)
          (res (pcase cmd
                 ,@(cl-loop
                    for (event args body) in (oref ioloop events)
                    for cond = (list '\` (cl-list* event (jupyter-ioloop--replace-args args)))
                    if (memq event '(quit callback timer))
                    do (error "Event can't be one of quit, callback, or, timer")
                    ;; cond = `(event ,arg1 ,arg2 ...)
                    else collect `(,cond ,@body))
                 ;; Default events
                 (`(timer ,id ,period ,cb)
                  ;; Ensure we don't send anything back to the parent process
                  (prog1 nil
                    (let ((timer (run-at-time 0.0 period (byte-compile cb))))
                      (puthash id timer jupyter-ioloop-timers))))
                 (`(callback ,cb)
                  ;; Ensure we don't send anything back to the parent process
                  (prog1 nil
                    (setq jupyter-ioloop-timeout 0)
                    (add-hook 'jupyter-ioloop-pre-hook (byte-compile cb) 'append)))
                 ('(quit) (signal 'quit nil))
                 (_ (error "Unhandled command %s" cmd)))))
     ;; Can only send lists at the moment
     (when (and res (listp res)) (zmq-prin1 res))))

(cl-defgeneric jupyter-ioloop-add-callback ((ioloop jupyter-ioloop) cb)
  "In IOLOOP, add CB to be run in the IOLOOP environment.
CB is run at the start of every polling loop. Callbacks are
called in the order they are added.

WARNING: A function added as a callback should be quoted to avoid
sending closures to the IOLOOP. An example:

    (jupyter-ioloop-add-callback ioloop
      `(lambda () (zmq-prin1 'foo \"bar\")))"
  (declare (indent 1))
  (cl-assert (functionp cb))
  (cl-callf append (oref ioloop callbacks) (list cb))
  (when (process-live-p (oref ioloop process))
    (jupyter-send ioloop 'callback (macroexpand-all cb))))

(defun jupyter-ioloop-poller-add (socket events)
  "Add SOCKET to be polled using the `jupyter-ioloop-poller'.
EVENTS are the polling events that should be listened for on SOCKET."
  (when (zmq-poller-p jupyter-ioloop-poller)
    (zmq-poller-add jupyter-ioloop-poller socket events)
    (cl-incf jupyter-ioloop-nsockets)))

(defun jupyter-ioloop-poller-remove (socket)
  "Remove SOCKET from the `jupyter-ioloop-poller'."
  (when (zmq-poller-p jupyter-ioloop-poller)
    (zmq-poller-remove jupyter-ioloop-poller socket)
    (cl-decf jupyter-ioloop-nsockets)))

(defun jupyter-ioloop--delete-process (process)
  (when-let* ((stdin (process-get process :stdin))
              (socket-p (zmq-socket-p stdin)))
    (zmq-close stdin)
    (process-put process :stdin nil))
  (delete-process process))

(defun jupyter-ioloop--function (ioloop port)
  "Return the function that does the work of IOLOOP.
The returned function is suitable to send to a ZMQ subprocess for
evaluation using `zmq-start-process'.

If PORT is non-nil the returned function will create a ZMQ PULL
socket to receive events from the parent process on the PORT of
the local host, otherwise events are expected to be received on
STDIN. This is useful on Windows systems which don't allow
polling the STDIN file handle."
  (let ((stdin-form
         (if port `(let ((sock (zmq-socket ctx zmq-PULL)))
                     (prog1 sock
                       (zmq-connect sock (format "tcp://127.0.0.1:%s" ,port))))
           '0))
        (dispatcher-form
         (jupyter-ioloop--event-dispatcher
          ioloop (if port '(read (zmq-recv-decoded jupyter-ioloop-stdin))
                   '(zmq-subprocess-read)))))
    `(lambda (ctx)
       (push ,(file-name-directory (locate-library "jupyter-base")) load-path)
       (require 'jupyter-ioloop)
       (setq jupyter-ioloop-poller (zmq-poller))
       (setq jupyter-ioloop-stdin ,stdin-form)
       (zmq-poller-add jupyter-ioloop-poller jupyter-ioloop-stdin zmq-POLLIN)
       (let (events)
         (condition-case nil
             (progn
               ,@(oref ioloop setup)
               (setq
                ;; Initialize any callbacks that were added before the ioloop was started
                jupyter-ioloop-pre-hook
                (mapcar (lambda (f) (unless (byte-code-function-p f) (byte-compile f)))
                   (append jupyter-ioloop-pre-hook
                           (quote ,(mapcar #'macroexpand-all
                                      (oref ioloop callbacks))))))
               ;; Notify the parent process we are ready to do something
               (zmq-prin1 '(start))
               (let ((dispatcher (byte-compile (lambda () ,dispatcher-form))))
                 (while t
                   (run-hooks 'jupyter-ioloop-pre-hook)
                   (setq events
                         (condition-case nil
                             (zmq-poller-wait-all
                              jupyter-ioloop-poller
                              jupyter-ioloop-nsockets
                              jupyter-ioloop-timeout)
                           ((zmq-EAGAIN zmq-EINTR zmq-ETIMEDOUT) nil)))
                   (let ((stdin-event (zmq-assoc jupyter-ioloop-stdin events)))
                     (when stdin-event
                       (setq events (delq stdin-event events))
                       (funcall dispatcher)))
                   (when events
                     (run-hook-with-args 'jupyter-ioloop-post-hook events)))))
           (quit
            ,@(oref ioloop teardown)
            (zmq-prin1 '(quit))))))))

(defun jupyter-ioloop-alive-p (ioloop)
  "Return non-nil if IOLOOP is ready to receive/send events."
  (cl-check-type ioloop jupyter-ioloop)
  (with-slots (process) ioloop
    (and (process-live-p process) (process-get process :start))))

(defun jupyter-ioloop--make-filter (ioloop ref)
  (lambda (event)
    (let ((obj (jupyter-weak-ref-resolve ref)))
      (if obj (jupyter-ioloop-handler ioloop obj event)
        (jupyter-ioloop--delete-process (oref ioloop process))))))

(cl-defgeneric jupyter-ioloop-start ((ioloop jupyter-ioloop)
                                     object
                                     &key buffer)
  "Start an IOLOOP.
OBJECT is an object which is used to dispatch on when the current
Emacs process receives an event to handle from IOLOOP, see
`jupyter-ioloop-handler'.

If IOLOOP was previously running, it is stopped first.

If BUFFER is non-nil it should be a buffer that will be used as
the IOLOOP subprocess buffer, see `zmq-start-process'."
  (jupyter-ioloop-stop ioloop)
  (let (stdin port)
    (when (memq system-type '(windows-nt ms-dos cygwin))
      (setq stdin (zmq-socket (zmq-current-context) zmq-PUSH))
      (setq port (zmq-bind-to-random-port stdin "tcp://127.0.0.1")))
    (let ((process (zmq-start-process
                    (jupyter-ioloop--function ioloop (when stdin port))
                    ;; We go through this Emacs-fu, brought to you by Chris
                    ;; Wellons, https://nullprogram.com/blog/2014/01/27/,
                    ;; because we want OBJECT to be the final say in when
                    ;; everything gets garbage collected. If OBJECT loses
                    ;; scope, the ioloop process should be killed off. This
                    ;; wouldn't happen if we hold a strong reference to
                    ;; OBJECT.
                    :filter (jupyter-ioloop--make-filter
                             ioloop (jupyter-weak-ref object))
                    :buffer buffer)))
      (oset ioloop process process)
      (when stdin
        (process-put process :stdin stdin))
      (jupyter-ioloop-wait-until ioloop 'start #'identity))))

(cl-defgeneric jupyter-ioloop-stop ((ioloop jupyter-ioloop))
  "Stop IOLOOP.
Send a quit event to IOLOOP, wait until it actually quits before
returning."
  (with-slots (process) ioloop
    (when (process-live-p process)
      (jupyter-send ioloop 'quit)
      (unless (jupyter-ioloop-wait-until ioloop 'quit #'identity)
        (delete-process process))
      (when-let* ((stdin (process-get process :stdin))
                  (socket-p (zmq-socket-p stdin)))
        (zmq-unbind stdin (zmq-get-option stdin zmq-LAST-ENDPOINT))))))

(cl-defmethod jupyter-send ((ioloop jupyter-ioloop) &rest args)
  "Using IOLOOP, send ARGS to its process.

All arguments passed to this function are sent as a list to the
process unchanged. This means that all arguments should be
serializable."
  (with-slots (process) ioloop
    (cl-assert (process-live-p process))
    (let ((stdin (process-get process :stdin)))
      (if stdin (zmq-send-encoded stdin (with-temp-buffer
                                          (prin1 args (current-buffer))
                                          (buffer-string)))
        (zmq-subprocess-send process args)))))

(provide 'jupyter-ioloop)

;;; jupyter-ioloop.el ends here
