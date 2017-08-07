(use s-sparql s-sparql-parser
     srfi-1 persistent-hash-map matchable)

(define (assp pred alist)
  (find (lambda (pair) (pred (car pair))) alist))

(include "fmicroKanren/fmicroKanren.scm")
(include "fmicroKanren/miniKanren-wrappers.scm")


(define-syntax project
  (syntax-rules ()
    ((_ (x ...) g)
     (lambda (s/c)
       (let ((x (walk x (car s/c))) ...)
	 (g s/c))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Database
(define-record db time tables)

(define-record dbs s sp p po o os spo)

(define (empty-index) (persistent-map))

(define (empty-db)
  (let ((time (cpu-time)))
    (make-db
     (list time)
     (persistent-map
      time
      (make-dbs (empty-index) (empty-index) (empty-index) (empty-index)
                (empty-index) (empty-index) (empty-index))))))

(define *DB* (make-parameter (empty-db)))

(define (latest DB)
  (map-ref (db-tables DB) (car (db-time DB))))

(define (appendor a b)
  (append (or a '()) (or b '())))

;; !! **
(define (consr lst elt)
  (if (and (list? lst) (member elt lst)) lst
      (cons elt (or lst '()))))

(define (update-triples triples val)
  (let* ((time (cpu-time))
        (DBS (latest (*DB*))))
    (let loop ((triples triples)
               (si (dbs-s DBS))
               (spi (dbs-sp DBS))
               (pi (dbs-p DBS))
               (poi (dbs-po DBS))
               (oi (dbs-o DBS))
               (osi (dbs-os DBS))
               (spoi (dbs-spo DBS)))
      (if (null? triples)
          (*DB*
           (make-db
            (cons time (db-time (*DB*)))
            (map-add (db-tables (*DB*)) time (make-dbs si spi pi poi oi osi spoi))))
          (match (car triples)
            ((s p o)
             (loop (cdr triples)
                   (map-update-in si `(,s) consr p)
                   (map-update-in spi `(,(symbol-append s p)) consr o)
                   (map-update-in pi `(,p) consr  o)
                   (map-update-in poi `(,(symbol-append p o)) consr s)
                   (map-update-in oi `(,o) consr s)
                   (map-update-in osi `(,(symbol-append o s)) consr p)
                   (map-add spoi (symbol-append s p o) val))))))))
                   ;; (map-update-in si `(,s) consr p)
                   ;; (map-update-in spi `((,s ,p)) consr o)
                   ;; (map-update-in pi `(,p) consr  o)
                   ;; (map-update-in poi `((,p ,o)) consr s)
                   ;; (map-update-in oi `(,o) consr s)
                   ;; (map-update-in osi `((,o ,s)) consr p)
                   ;; (map-add spoi `(,s ,p ,o) val))))))))


(define add-triples (cut update-triples <> #t))

(define delete-triples (cut update-triples <> #f))

(define (add-triple s p o)
  (add-triples `((,s ,p ,o))))

(define (delete-triple s p o)
  (delete-triples `((,s ,p ,o))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; db-kanren interface
(define (tripleo s p o)
  (let ((U (lambda (table keys var)
	     (let ((vals (map-ref (table (latest (*DB*))) keys)))
	       (let stream ((reference '())
			    (next-ref vals)
			    (os vals))
		 (if (equal? os reference)
		     (later
		      (let ((vals (map-ref (table (latest (*DB*))) keys)))
			(stream next-ref vals vals)))
		     (disj
		      (conj (== var (car os))
			    (project (s p o)
				     (tripleo s p o)))
		      (stream reference next-ref (cdr os)))))))))
    (cond ((and (var? s) (var? p) (var? o)) 5)
	  ((and (var? s) (var? p)) (U dbs-o o s))
	  ((and (var? s) (var? o)) (U dbs-p p o))
	  ((and (var? p) (var? o)) (U dbs-s s p))
	  ((var? s) (U dbs-po (symbol-append p o) s))
	  ((var? p) (U dbs-os (symbol-append o s) p))
	  ((var? o) (U dbs-sp (symbol-append s p) o))
	  (else (let ((val (lambda ()
			     (map-ref (dbs-spo (latest (*DB*))) (symbol-append s p o)))))
		  (disj 
		   (== (val) #t)
		   (later (== (val) #f))))))))
;;			(later (== (val) #t))))))))
		    
;;(disj
;;		   (== val #t)  )))))
		;	(delay
		;	  (==  (map-ref (dbs-spo (latest (*DB*))) (list s p o)) #t))))))))

(add-triples '((<S> <P> <O>)
	       (<U> <V> <O>)
	       (<S> <P> <O2>)
	       (<Q> <R> <O>)))

;; (define r  ((tripleo '<S> '<P> #(0)) empty-state))
;; (define f (drop r 2))
;; (add-triples '((<S> <P> <OOO>)))

(define r ((conj (tripleo '<Q> '<R> #(0))
		 (conj (tripleo '<S> '<P> #(0))
		       (tripleo '<U> '<V> #(0)))
		 
		 )
                 empty-state))

(define r2 ((conj+ (tripleo '<Q> '<R> #(0))
		   (tripleo '<S> '<P> #(0))
		   (tripleo '<U> '<V> #(0)))
                 empty-state))
(add-triples '((<S> <P> <O3>)
	       (<Q> <R> <O3>)
	       (<U> <V> <O3>)
	       (<S> <P> <M>)
	       (<U> <V> <M>)
	       (<Q> <R> <M>)))

(print (promised r))
