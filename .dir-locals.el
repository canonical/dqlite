;;; Directory Local Variables
;;; For more information see (info "(emacs) Directory Variables")
((go-mode
  . ((go-test-args . "-tags libsqlite3")
     (eval
      . (let* ((locals-path
		(let ((d (dir-locals-find-file ".")))
		  (if (stringp d) (file-name-directory d) (car d))))
	       (sqlite-path (concat locals-path ".sqlite/")))
	  (set
	   (make-local-variable 'go-command)
	   (s-concat
	    "env "
	    "CGO_CFLAGS=-I" sqlite-path " "
	    "CGO_LDFLAGS=-L" sqlite-path " "
	    "LD_LIBRARY_PATH=" sqlite-path " "
	    "go")))))))
