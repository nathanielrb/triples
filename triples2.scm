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

(define-syntax later
  (syntax-rules ()
    ((_ g) (let rec () (disj g (next (rec)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Database
(define-record incrementals s sp p po o os spo)

(define (empty-incrementals)
  (apply make-incrementals
         (make-list 7 (persistent-map))))

(define-record incremental map list)

(define (empty-table)
  (make-incremental (persistent-map) '()))

(define (update-incrementals incrementals key)
  (let ((incrementals (or incrementals (empty-table))))
    (if (map-ref (incremental-map incrementals) key)
	incrementals
	(make-incremental 
	 (map-add (incremental-map incrementals) key #t)
	 (cons key (incremental-list incrementals))))))

(define *DB* (list (empty-incrementals)))

(define (latest-db) (car *DB*))

(define (latest-incrementals #!optional table key)
  (if table
      (incremental-list
       (map-ref (table (latest-db)) key (empty-table)))
      (map-keys (incrementals-s (latest-db)))))

(define (latest-triple s p o)
  (map-ref (incrementals-spo (latest-db))
	   (list s p o)))

(define (update-triples DB triples val)
  (let ((DBS (car DB)))
    (let loop ((triples triples)
               (i/s (incrementals-s DBS))
               (i/sp (incrementals-sp DBS))
               (i/p (incrementals-p DBS))
               (i/po (incrementals-po DBS))
               (i/o (incrementals-o DBS))
               (i/os (incrementals-os DBS))
               (i/spo (incrementals-spo DBS)))
      (if (null? triples)
	  (cons (make-incrementals i/s i/sp i/p i/po i/o i/os i/spo)
		DB)
          (match (car triples)
            ((s p o)
             (loop (cdr triples)
                   (map-update-in i/s `(,s) update-incrementals p)
                   (map-update-in i/sp `((,s ,p)) update-incrementals o)
                   (map-update-in i/p `((,p)) update-incrementals  o)
                   (map-update-in i/po `((,p ,o)) update-incrementals s)
                   (map-update-in i/o `((,o)) update-incrementals s)
                   (map-update-in i/os `((,o ,s)) update-incrementals p)
                   (map-add i/spo (list s p o) val))))))))

(define (add-triples DB triples) (update-triples DB triples #t))

(define (delete-triples DB triples) (update-triples DB triples #f))

(define (add-triple s p o)
  (add-triples `((,s ,p ,o))))

(define (delete-triple s p o)
  (delete-triples `((,s ,p ,o))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Goal Constructors
(define (tripleo s p o)
  (let ((mkstrm (lambda (var get-indexes)
		  (let stream ((indexes (get-indexes)))
		      (if (null? indexes) (lambda (s/c) mzero)
			  (disj
			   (conj (== var (car indexes))
				 (project (s p o) (tripleo s p o)))
			   (stream (cdr indexes))))))))
    (cond ((and (var? s) (var? p) (var? o)) (mkstrm s (lambda () (latest-incrementals))))
	  ((and (var? s) (var? p))          (mkstrm s (lambda () (latest-incrementals incrementals-o o))))
	  ((and (var? s) (var? o))          (mkstrm o (lambda () (latest-incrementals incrementals-p p))))
	  ((and (var? p) (var? o))          (mkstrm p (lambda () (latest-incrementals incrementals-s s))))
	  ((var? s)                         (mkstrm s (lambda () (latest-incrementals incrementals-po (list p o)))))
	  ((var? p)                         (mkstrm p (lambda () (latest-incrementals incrementals-os (list o s)))))
	  ((var? o)                         (mkstrm o (lambda () (latest-incrementals incrementals-sp (list s p)))))
	  (else (== #t (latest-triple s p o))))))

(define (triple-nolo delta s p o)
  (let ((mkstrm (lambda (var get-indexes)
		  (let ((indexes (get-indexes)))
		    (let stream ((indexes indexes) (ref '()) (next-ref indexes))
		      (if (equal? indexes ref)
			  (next
			   (let ((vals (get-indexes)))
			     (stream vals next-ref vals)))
			  (disj
			   (conj (== var (car indexes))
				 (project (delta s p o) (triple-nolo delta s p o)))
			   (stream (cdr indexes) ref next-ref))))))))
    (cond ((and (var? s) (var? p) (var? o)) (mkstrm s (lambda () (latest-incrementals))))
	  ((and (var? s) (var? p))          (mkstrm s (lambda () (latest-incrementals incrementals-o o))))
	  ((and (var? s) (var? o))          (mkstrm o (lambda () (latest-incrementals incrementals-p p))))
	  ((and (var? p) (var? o))          (mkstrm p (lambda () (latest-incrementals incrementals-s s))))
	  ((var? s)                         (mkstrm s (lambda () (latest-incrementals incrementals-po (list p o)))))
	  ((var? p)                         (mkstrm p (lambda () (latest-incrementals incrementals-os (list o s)))))
	  ((var? o)                         (mkstrm o (lambda () (latest-incrementals incrementals-sp (list s p)))))
	  (else
           (let leaf ((ref #f))
             (let ((v (latest-triple s p o)))
               (cond ((eq? v ref) (next (leaf v)))
		     (v (disj (== delta '+) (next (leaf v))))
                     (else (disj (== delta '-) (next (leaf v)))))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests
;; (define DB (empty-db))


(define r (run* (q)
           (fresh (o delta d1 d2 d3) 
             (== q `(,delta  ,o))
	     (== delta `(,d1 ,d2 ,d3))
             (triple-nolo  d1 '<Q> '<R> o)
             (triple-nolo  d2  '<S> '<P> o)
             (triple-nolo  d3 '<U> '<V> o))))


(print r)


(set! *DB*
   (add-triples *DB*  '((<S> <P> <O>)
		   (<U> <V> <O>)
		   (<S> <P> <O2>)
		   (<Q> <R> <O>)
		   (<A> <B> <C>))))

 
(print (advance r))

;; (define DB3
(set! *DB* (delete-triples *DB* '((<S> <P> <O>))))

(print (advance (advance r)))

;; (define DB4
(set! *DB*  (add-triples *DB* '((<S> <P> <O3>)
				(<S> <P> <O>)
				(<Q> <R> <O3>)
				(<U> <V> <O3>)
				(<S> <P> <M>)
				(<U> <V> <M>)
				(<Q> <R> <M>))))

(print (advance (advance (advance r))))

;;;

;; (print "***")
;; (define r3 (run* (o)
;;              (tripleo '<Q> '<R> o)
;;              (tripleo  '<S> '<P> o)
;;              (tripleo '<U> '<V> o)))

;; (print r3)




;; ;;;

;; (set! *DB*
;;    (add-triples *DB* '((<A> <B> <C>))))

;; (define r2 (run* (o)
;; 		 (tripleo '<A> '<B> o)
;; 		 (later (tripleo  '<X> '<Y> o))))

;; (print "r2: " r2)

;; (set! *DB*
;;   (add-triples *DB* '((<X> <Y> <C>))))

;; (print "r2: " (advance r2))

