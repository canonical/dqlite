;;; Directory Local Variables
;;; For more information see (info "(emacs) Directory Variables")
(
 (go-mode
  . ((eval
      . (let
	    ((sqlite-path
	      (concat (file-name-directory (dir-locals-find-file ".")) ".sqlite/")))
	  (progn
	    (setenv "CGO_CFLAGS" (concat "-I" sqlite-path))
	    (setenv "CGO_LDFLAGS" (concat "-L" sqlite-path))
	    (setenv "LD_PRELOAD_PATH" sqlite-path))))
     (go-test-args . "-tags libsqlite3"))))
