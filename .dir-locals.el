;;; Directory Local Variables
;;; For more information see (info "(emacs) Directory Variables")
(
 (go-mode
  . ((go-test-args . "-tags libsqlite3")
     (eval
      . (let* ((locals-path (let ((d (dir-locals-find-file "."))) (if (stringp d) (file-name-directory d) (car d))))
	       (sqlite-path (concat locals-path ".sqlite/")))
 	  (progn
 	    (setenv "CGO_CFLAGS" (concat "-I" sqlite-path))
 	    (setenv "CGO_LDFLAGS" (concat "-L" sqlite-path))
 	    (setenv "LD_LIBRARY_PATH" sqlite-path))))
     )))
