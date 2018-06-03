(use srfi-1 persistent-hash-map matchable)

(define (assp pred alist)
  (find (lambda (pair) (pred (car pair))) alist))

;; (include "fmicroKanren/fmicroKanren.scm")
;; (include "fmicroKanren/miniKanren-wrappers.scm")

(include "ftmicroKanren/ftmicroKanren.scm")
(include "ftmicroKanren/miniKanren-wrappers.scm")

(define-syntax project
  (syntax-rules ()
    ((_ (x ...) g)
     (lambda (s/c)
       (let ((x (walk x (car s/c))) ...)
	 (g s/c))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Database
;; tied together with lists, persistent hash maps, and shoestring
;;
;; basic structure with [key => val] for persistent hash maps and <name -slot: val> for records:
;; <database
;;   -time: (T_n T_(n-1) ... T_0)
;;   -indexes: [ T_0 => <index -s  [<S> => (<P1> <P2> ...)  ]
;;                            -sp [(<S> <P1>) => (O11 O12 ...)  ]
;;                            -p -po -o -os -spo>
;;               T_1 => <index> ] >
(define-record incrementals s sp p po o os spo)

(define (empty-incrementals)
  (apply make-incrementals
         (make-list 7 (persistent-map))))

(print (incrementals-s (empty-incrementals)))

(define-record incremental map list)

(define (empty-incremental)
  (make-incremental (persistent-map) '()))

(define (update-incrementals incrementals elt)
  (let ((incrementals (or incrementals (empty-incremental))))
  (if (map-ref (incremental-map incrementals) elt)
      incrementals
      (make-incremental 
       (map-add (incremental-map incrementals) elt #t)
       (cons elt (incremental-list incrementals))))))


(define-record db time incrementals)

(define (empty-db)
  (let ((time (cpu-time)))
    (make-db
     (list time)
     (persistent-map
      time
      (empty-incrementals)))))

(define *DB* (make-parameter (empty-db)))

(define (latest-db)
  (map-ref (db-incrementals (*DB*)) (car (db-time (*DB*)))))

;; (define (latest-db DB)
;;   (map-ref (db-incrementals DB) (car (db-time DB))))

;; (define (consr lst elt) ;; shoestring...
;;   (if (and (list? lst) (member elt lst)) lst
;;       (cons elt (or lst '()))))

(define incrementals-key list)

(define (update-triples triples val)
  (let* ((time (cpu-time))
        (DBS (latest-db)))
    (let loop ((triples triples)
               (i/s (incrementals-s DBS))
               (i/sp (incrementals-sp DBS))
               (i/p (incrementals-p DBS))
               (i/po (incrementals-po DBS))
               (i/o (incrementals-o DBS))
               (i/os (incrementals-os DBS))
               (i/spo (incrementals-spo DBS)))
      (if (null? triples)
          (*DB*
           (make-db
            (cons time (db-time (*DB*)))
            (map-add (db-incrementals (*DB*)) time (make-incrementals i/s i/sp i/p i/po i/o i/os i/spo))))
          (match (car triples)
            ((s p o)
             (loop (cdr triples)
                   (map-update-in i/s `(,s) update-incrementals p)
                   (map-update-in i/sp `(,(incrementals-key s p)) update-incrementals o)
                   (map-update-in i/p `(,p) update-incrementals  o)
                   (map-update-in i/po `(,(incrementals-key p o)) update-incrementals s)
                   (map-update-in i/o `(,o) update-incrementals s)
                   (map-update-in i/os `(,(incrementals-key o s)) update-incrementals p)
                   (map-add i/spo (incrementals-key s p o) val))))))))

(define (add-triples triples) (update-triples triples #t))

(define (delete-triples triples) (update-triples triples #f))

(define (add-triple s p o)
  (add-triples `((,s ,p ,o))))

(define (delete-triple s p o)
  (delete-triples `((,s ,p ,o))))

(define (triple-nol delta s p o)
  (let ((get-incrementals (lambda (table key)
                            (lambda ()
                              (incremental-list
                               (map-ref (table (latest-db)) key)))))
        (T (lambda (index var)
	     (let ((vals (index)))
	       (let stream ((os vals) (ref '()) (next-ref vals))
		 (if (equal? os ref)
		     (next
		      (let ((vals (index)))
                        (stream vals next-ref vals)))
		     (disj
		      (conj (== var (car os))
			    (project (s p o) (triple-nol delta s p o)))
		      (stream (cdr os) ref next-ref))))))))
    (cond ((and (var? s) (var? p) (var? o)) (T (compose map-keys incrementals-s) s))
	  ((and (var? s) (var? p))          (T (get-incrementals incrementals-o o) s))
	  ((and (var? s) (var? o))          (T (get-incrementals incrementals-p p) o))
	  ((and (var? p) (var? o))          (T (get-incrementals incrementals-s s) p))
	  ((var? s)                         (T (get-incrementals incrementals-po (incrementals-key p o)) s))
	  ((var? p)                         (T (get-incrementals incrementals-os (incrementals-key o s)) p))
	  ((var? o)                         (T (get-incrementals incrementals-sp (incrementals-key s p)) o))
	  (else
           (let singleton ((ref #f))
             (let ((v (map-ref (incrementals-spo (latest-db)) (incrementals-key s p o))))
               (cond ((eq? v ref) (next (singleton v)))
                     (v (disj (== #t #t) (next (singleton v))))
                     (else (disj (== delta '-) (next (singleton v)))))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests
;; (define DB (empty-db))

;; (define DB2
  (add-triples  '((<S> <P> <O>)
                    (<U> <V> <O>)
                    (<S> <P> <O2>)
                    (<Q> <R> <O>)))

;; (print DB2)

(define r (run* (q)
           (fresh (o delta) 
             (== q `(,delta ,o))
             (triple-nol  delta '<Q> '<R> o)
             (triple-nol  delta '<S> '<P> o)
             (triple-nol  delta '<U> '<V> o))))

(print r)

;; (define DB3
  (delete-triples '((<S> <P> <O>)))

(print (advance r))

;; (define DB4
  (add-triples '((<S> <P> <O3>)
                 (<S> <P> <O>)
                 (<Q> <R> <O3>)
                 (<U> <V> <O3>)
                 (<S> <P> <M>)
                 (<U> <V> <M>)
                 (<Q> <R> <M>)))

(print (advance (advance r)))
