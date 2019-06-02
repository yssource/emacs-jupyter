;;; jupyter-channels.el --- Jupyter channels -*- lexical-binding: t -*-

;; Copyright (C) 2018-2019 Nathaniel Nicandro

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

;; Implements synchronous channel types. Each channel is essentially a wrapper
;; around a `zmq-socket' constrained to a socket type by the type of the
;; channel and with an associated `zmq-IDENTITY' obtained from the
;; `jupyter-session' that must be associated with the channel. A heartbeat
;; channel is distinct from the other channels in that it is implemented using
;; a timer which periodically pings the kernel depending on how its configured.
;; In order for communication to occur on the other channels, one of
;; `jupyter-send' or `jupyter-recv' must be called after starting the channel
;; with `jupyter-start-channel'.

;;; Code:

(eval-when-compile (require 'subr-x))
(require 'jupyter-base)
(require 'jupyter-messages)             ; For `jupyter-send'
(require 'ring)

(defgroup jupyter-channels nil
  "Jupyter channels"
  :group 'jupyter)

(defcustom jupyter-hb-max-failures 5
  "Number of heartbeat failures until the kernel is considered unreachable.
A ping is sent to the kernel on a heartbeat channel and waits
until `time-to-dead' seconds to see if the kernel sent a ping
back. If the kernel doesn't send a ping back after
`jupyter-hb-max-failures', the callback associated with the
heartbeat channel is called. See `jupyter-hb-on-kernel-dead'."
  :type 'integer
  :group 'jupyter-channels)

;;; Basic channel types

(defclass jupyter-channel ()
  ((type
    :type keyword
    :initarg :type
    :documentation "The type of this channel. Should be one of
the keys in `jupyter-socket-types'.")
   (session
    :type jupyter-session
    :initarg :session
    :documentation "The session object used to sign and
send/receive messages.")
   (endpoint
    :type string
    :initarg :endpoint
    :documentation "The endpoint this channel is connected to.
 Typical endpoints look like \"tcp://127.0.0.1:5555\"."))
  :abstract t)

(defclass jupyter-sync-channel (jupyter-channel)
  ((socket
    :type (or null zmq-socket)
    :initform nil
    :documentation "The socket used for communicating with the kernel.")))

(cl-defgeneric jupyter-start-channel ((channel jupyter-channel) &key identity)
  "Start a Jupyter CHANNEL using IDENTITY as the routing ID.
If CHANNEL is already alive, do nothing.")

(cl-defmethod jupyter-start-channel ((channel jupyter-sync-channel)
                                     &key (identity (jupyter-session-id
                                                     (oref channel session))))
  (unless (jupyter-channel-alive-p channel)
    (let ((socket (jupyter-connect-channel
                   (oref channel type) (oref channel endpoint) identity)))
      (oset channel socket socket)
      (cl-case (oref channel type)
        (:iopub
         (zmq-socket-set socket zmq-SUBSCRIBE ""))))))

(cl-defgeneric jupyter-stop-channel ((channel jupyter-channel))
  "Stop a Jupyter CHANNEL.
If CHANNEL is already stopped, do nothing.")

(cl-defmethod jupyter-stop-channel ((channel jupyter-sync-channel))
  (when (jupyter-channel-alive-p channel)
    (zmq-socket-set (oref channel socket) zmq-LINGER 0)
    (zmq-close (oref channel socket))
    (oset channel socket nil)))

(cl-defmethod jupyter-send ((channel jupyter-sync-channel) type message &optional msg-id)
  (jupyter-send (oref channel session) (oref channel socket) type message msg-id))

(cl-defmethod jupyter-recv ((channel jupyter-sync-channel))
  (jupyter-recv (oref channel session) (oref channel socket)))

(cl-defgeneric jupyter-channel-alive-p ((channel jupyter-channel))
  "Determine if a CHANNEL is alive.")

(cl-defmethod jupyter-channel-alive-p ((channel jupyter-sync-channel))
  (not (null (oref channel socket))))

;;; Heartbeat channel

(defclass jupyter-hb-channel (jupyter-sync-channel)
  ((type
    :type keyword
    :initform :hb
    :documentation "The type of this channel is `:hb'.")
   (time-to-dead
    :type number
    :initform 1
    :documentation "The time in seconds to wait for a response
from the kernel until the connection is assumed to be dead. Note
that this slot only takes effect when starting the channel.")
   (dead-cb
    :type function
    :initform #'ignore
    :documentation "A callback function that takes 0 arguments
and is called when the kernel has not responded for
\(* `jupyter-hb-max-failures' `time-to-dead'\) seconds.")
   (beating
    :type (or boolean symbol)
    :initform t
    :documentation "A flag variable indicating that the heartbeat
channel is communicating with the kernel.")
   (paused
    :type boolean
    :initform t
    :documentation "A flag variable indicating that the heartbeat
channel is paused and not communicating with the kernel. To
pause the heartbeat channel use `jupyter-hb-pause', to unpause
use `jupyter-hb-unpause'."))
  :documentation "A base class for heartbeat channels.")

(cl-defmethod jupyter-channel-alive-p ((channel jupyter-hb-channel))
  "Return non-nil if CHANNEL is alive."
  (zmq-socket-p (oref channel socket)))

(defun jupyter-hb--pingable-p (channel)
  (and (not (oref channel paused))
       (jupyter-channel-alive-p channel)))

(cl-defmethod jupyter-hb-beating-p ((channel jupyter-hb-channel))
  "Return non-nil if CHANNEL is reachable."
  (and (jupyter-hb--pingable-p channel)
       (oref channel beating)))

(cl-defmethod jupyter-hb-pause ((channel jupyter-hb-channel))
  "Pause checking for heartbeat events on CHANNEL."
  (oset channel paused t))

(cl-defmethod jupyter-hb-unpause ((channel jupyter-hb-channel))
  "Un-pause checking for heatbeat events on CHANNEL."
  (when (oref channel paused)
    (if (jupyter-channel-alive-p channel)
        ;; Consume a pending message from the kernel if there is one. We send a
        ;; ping and then schedule a timer which fires TIME-TO-DEAD seconds
        ;; later to receive the ping back from the kernel and start the process
        ;; all over again. If the channel is paused before TIME-TO-DEAD
        ;; seconds, there may still be a ping from the kernel waiting.
        (ignore-errors (zmq-recv (oref channel socket) zmq-DONTWAIT))
      (jupyter-start-channel channel))
    (oset channel paused nil)
    (jupyter-hb--send-ping channel)))

(cl-defmethod jupyter-hb-on-kernel-dead ((channel jupyter-hb-channel) fun)
  "When the kernel connected to CHANNEL dies, call FUN.
A kernel is considered dead when CHANNEL does not receive a
response after \(* `jupyter-hb-max-failures' `time-to-dead'\)
seconds has elapsed without the kernel sending a ping back."
  (declare (indent 1))
  (oset channel dead-cb fun))

(defun jupyter-hb--send-ping (channel &optional failed-count)
  (when (jupyter-hb--pingable-p channel)
    (zmq-send (oref channel socket) "ping")
    (run-with-timer
     (oref channel time-to-dead) nil
     (lambda (channel-ref)
       (when-let* ((channel (jupyter-weak-ref-resolve channel-ref))
                   (sock (and (jupyter-hb--pingable-p channel)
                              (oref channel socket))))
         (oset channel beating
               (condition-case nil
                   (and (zmq-recv sock zmq-DONTWAIT) t)
                 ((zmq-EINTR zmq-EAGAIN) nil)))
         (if (oref channel beating)
             (jupyter-hb--send-ping channel)
           ;; Reset the socket
           (jupyter-stop-channel channel)
           (jupyter-start-channel channel)
           (or failed-count (setq failed-count 0))
           (if (< failed-count jupyter-hb-max-failures)
               (jupyter-hb--send-ping channel (1+ failed-count))
             (oset channel paused t)
             (when (functionp (oref channel dead-cb))
               (funcall (oref channel dead-cb)))))))
     (jupyter-weak-ref channel))))

(provide 'jupyter-channels)

;;; jupyter-channels.el ends here
