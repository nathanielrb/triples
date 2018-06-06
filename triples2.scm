(use srfi-1 persistent-hash-map matchable)

(define (assp pred alist)
  (find (lambda (pair) (pred (car pair))) alist))

(include "ftmicroKanren/ftmicroKanren.scm")
(include "ftmicroKanren/miniKanren-wrappers.scm")

(define-syntax project
  (syntax-rules ()
    ((_ (x ...) g)
     (lambda (s/c)
       (let ((x (walk x (car s/c))) ...)
	 (g s/c))))))

(define-syntax eventually
  (syntax-rules ()
    ((_ g) (let rec () (disj g (next (rec)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Database
(define-record db s sp p po o os spo)

(define-record incrementals map list)

(define (empty-incrementals)
  (make-incrementals (persistent-map) '()))

(define (empty-db)
  (apply make-db
	 (make-list 7 (persistent-map))))

(define (update-incrementals table key val)
  (let ((incrementals (map-ref table key (empty-incrementals))))
    (map-add table key
	     (if (map-ref (incrementals-map incrementals) val)
		 incrementals
		 (make-incrementals
		  (map-add (incrementals-map incrementals) val #t)
		  (cons val (incrementals-list incrementals)))))))

(define (update-triple table triple val)
  (map-add table triple val))

(define latest-db (make-parameter (empty-db)))

(define (latest-incrementals accessor key)
  (incrementals-list
   (map-ref (accessor (latest-db)) key (empty-incrementals))))

(define (latest-triple s p o)
  (map-ref (db-spo (latest-db))
	   (list s p o)))

(define (update-triples DB triples val)
  (let loop ((triples triples)
	     (i/s (db-s DB))
	     (i/sp (db-sp DB))
	     (i/p (db-p DB))
	     (i/po (db-po DB))
	     (i/o (db-o DB))
	     (i/os (db-os DB))
	     (i/spo (db-spo DB)))
    (if (null? triples)
	(make-db i/s i/sp i/p i/po i/o i/os i/spo)
	(match (car triples)
	  ((s p o)
	   (loop (cdr triples)
		 (update-incrementals
		  (update-incrementals i/s #f s) 
		  s p)
		 (update-incrementals i/sp (list s p) o)
		 (update-incrementals i/p p o)
		 (update-incrementals i/po (list p o) s)
		 (update-incrementals i/o o s)
		 (update-incrementals i/os (list o s) p)
		 (update-triple i/spo (list s p o) val)))))))

(define (add-triples DB triples) (update-triples DB triples #t))

(define (delete-triples DB triples) (update-triples DB triples #f))

(define (add-triple s p o)
  (add-triples `((,s ,p ,o))))

(define (delete-triple s p o)
  (delete-triples `((,s ,p ,o))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Goal Constructors
(define (tripleo s p o)
  (let ((mkstrm (lambda (var accessor key)
		  (let ((get-incrementals (lambda ()
					    (latest-incrementals accessor key))))
		    (let stream ((indexes (get-incrementals)))
		      (if (null? indexes) (lambda (s/c) mzero)
			  (disj
			   (conj (== var (car indexes))
				 (project (s p o) (tripleo s p o)))
			   (stream (cdr indexes)))))))))
    (cond ((and (var? s) (var? p) (var? o)) (mkstrm s db-s #f))
	  ((and (var? s) (var? p))          (mkstrm s db-o o))
	  ((and (var? s) (var? o))          (mkstrm o db-p p))
	  ((and (var? p) (var? o))          (mkstrm p db-s s))
	  ((var? s)                         (mkstrm s db-po (list p o)))
	  ((var? p)                         (mkstrm p db-os (list o s)))
	  ((var? o)                         (mkstrm o db-sp (list s p)))
	  (else (== #t (latest-triple s p o))))))

(define (triple-nolo delta s p o)
  (let ((mkstrm (lambda (var accessor key)
		  (let* ((get-incrementals (lambda ()
					    (latest-incrementals accessor key)))
			(indexes (get-incrementals)))
		    (let stream ((indexes indexes) (ref '()) (next-ref indexes))
		      (if (equal? indexes ref)
			  (next
			   (let ((vals (get-incrementals)))
			     (stream vals next-ref vals)))
			  (disj
			   (conj (== var (car indexes))
				 (project (delta s p o) (triple-nolo delta s p o)))
			   (stream (cdr indexes) ref next-ref))))))))
    (cond ((and (var? s) (var? p) (var? o)) (mkstrm s db-s #f))
	  ((and (var? s) (var? p))          (mkstrm s db-o o))
	  ((and (var? s) (var? o))          (mkstrm o  db-p p))
	  ((and (var? p) (var? o))          (mkstrm p db-s s))
	  ((var? s)                         (mkstrm s db-po (list p o)))
	  ((var? p)                         (mkstrm p db-os (list o s)))
	  ((var? o)                         (mkstrm o db-sp (list s p)))
	  (else
           (let leaf ((ref #f))
             (let ((v (latest-triple s p o)))
               (cond ((eq? v ref) (next (leaf v)))
		     (v (disj (== delta '+) (next (leaf v))))
                     (else (disj (== delta '-) (next (leaf v)))))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests
;; (define DB (empty-db))
(define db0 (empty-db))

(define r 
  (parameterize ((latest-db db0))
    (run* (q)
	  (fresh (o delta d1 d2 d3) 
		 (== q `(,delta  ,o))
		 (== delta `(,d1 ,d2 ,d3))
		 (triple-nolo  d1 '<Q> '<R> o)
		 (triple-nolo  d2  '<S> '<P> o)
		 (triple-nolo  d3 '<U> '<V> o)))))


(print r)


(define db1
  (add-triples db0  '((<S> <P> <O>)
		      (<U> <V> <O>)
		      (<S> <P> <O2>)
		      (<Q> <R> <O>)
		      (<A> <B> <C>))))

 
(parameterize ((latest-db db1))
  (print (advance r)))

(define db2 (delete-triples db1 '((<S> <P> <O>))))

(parameterize ((latest-db db2))
  (print (advance (advance r))))

(define db3
  (add-triples db2 '((<S> <P> <O3>)
		     (<S> <P> <O>)
		     (<Q> <R> <O3>)
		     (<U> <V> <O3>)
		     (<S> <P> <M>)
		     (<U> <V> <M>)
		     (<Q> <R> <M>))))

(parameterize ((latest-db db3))
  (print (advance (advance (advance r)))))

;;;

(print "***")
(parameterize ((latest-db db1))
  (define r3 (run* (q)
		   (triple-nolo '+ '<Q> '<R> q)
		   (triple-nolo '+ '<S> '<P> q)
		   (triple-nolo '+ '<U> '<V> q)))
  (print r3)
  (print (advance r3)))

;;;;
;; Problem
;; with triple-nolo, results are multiplied!

(define dba
  (add-triples db0 '((<A> <B> <C>))))

(parameterize ((latest-db dba))
  (define r2 (run* (o)
		   (eventually (tripleo  '<X> '<Y> o))
		   (tripleo '<A> '<B> o)))
		   
  (print "r2: " r2)

  (define dbb
    (add-triples dba '((<X> <Y> <C>))))

  (parameterize ((latest-db dbb))
    (print "r2: " (advance r2))
    (print "r2: " (advance (advance (advance r2))))))


(define *db*
(let loop ((i 0) (db (empty-db)) (o (gensym)))
  (if (= i 10000) db
      (let ((s (gensym)))
	(loop (+ i 1)
	      (add-triples db `((,s <P> ,o)))
	      s))))
)
