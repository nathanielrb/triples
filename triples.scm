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
;; tied together with lists, persistent hash maps, and shoestring
;;
;; basic structure with [key => val] for persistent hash maps and <name -slot: val> for records:
;; <database
;;   -time: (T_n T_(n-1) ... T_0)
;;   -indexes: [ T_0 => <index -s  [<S> => (<P1> <P2> ...)  ]
;;                            -sp [(<S> <P1>) => (O11 O12 ...)  ]
;;                            -p -po -o -os -spo>
;;               T_1 => <index> ] >
(define-record db time indexes)

(define-record index s sp p po o os spo)

(define (empty-index) (persistent-map))

(define (empty-db)
  (let ((time (cpu-time)))
    (make-db
     (list time)
     (persistent-map
      time
      (apply make-index (make-list 7 (empty-index)))))))

(define *DB* (make-parameter (empty-db)))

(define (latest-db)
  (map-ref (db-indexes (*DB*)) (car (db-time (*DB*)))))

(define (appendor a b)
  (append (or a '()) (or b '())))

(define (consr lst elt) ;; shoestring...
  (if (and (list? lst) (member elt lst)) lst
      (cons elt (or lst '()))))

(define index-key list)

(define (update-triples triples val)
  (let* ((time (cpu-time))
        (DBS (latest-db)))
    (let loop ((triples triples)
               (i/s (index-s DBS))
               (i/sp (index-sp DBS))
               (i/p (index-p DBS))
               (i/po (index-po DBS))
               (i/o (index-o DBS))
               (i/os (index-os DBS))
               (i/spo (index-spo DBS)))
      (if (null? triples)
          (*DB*
           (make-db
            (cons time (db-time (*DB*)))
            (map-add (db-indexes (*DB*)) time (make-index i/s i/sp i/p i/po i/o i/os i/spo))))
          (match (car triples)
            ((s p o)
             (loop (cdr triples)
                   (map-update-in i/s `(,s) consr p)
                   (map-update-in i/sp `(,(index-key s p)) consr o)
                   (map-update-in i/p `(,p) consr  o)
                   (map-update-in i/po `(,(index-key p o)) consr s)
                   (map-update-in i/o `(,o) consr s)
                   (map-update-in i/os `(,(index-key o s)) consr p)
                   (map-add i/spo (index-key s p o) val))))))))

(define add-triples (cut update-triples <> #t))

(define delete-triples (cut update-triples <> #f))

(define (add-triple s p o)
  (add-triples `((,s ,p ,o))))

(define (delete-triple s p o)
  (delete-triples `((,s ,p ,o))))

(define (triple-nol delta s p o)
  (let ((index-getter (lambda (table key)
                        (lambda () (map-ref (table (latest-db)) key))))
        (T (lambda (index var)
	     (let ((vals (index)))
	       (let stream ((os vals) (ref '()) (next-ref vals))
		 (if (equal? os ref)
		     (later
		      (let ((vals (index)))
                        (stream vals next-ref vals)))
		     (disj
		      (conj (== var (car os))
			    (project (s p o) (triple-nol delta s p o)))
		      (stream (cdr os) ref next-ref))))))))
    (cond ((and (var? s) (var? p) (var? o)) (T (compose map-keys index-s) s))
	  ((and (var? s) (var? p))          (T (index-getter index-o o) s))
	  ((and (var? s) (var? o))          (T (index-getter index-p p) o))
	  ((and (var? p) (var? o))          (T (index-getter index-s s) p))
	  ((var? s)                         (T (index-getter index-po (index-key p o)) s))
	  ((var? p)                         (T (index-getter index-os (index-key o s)) p))
	  ((var? o)                         (T (index-getter index-sp (index-key s p)) o))
	  (else
           (let singleton ((ref #f))
             (let ((v ((index-getter index-spo (index-key s p o)))))
               (cond ((eq? v ref) (later (singleton v)))
                     (v (disj (== #t #t) (later (singleton v))))
                     (else (disj (== delta '-) (later (singleton v)))))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests
(add-triples '((<S> <P> <O>)
	       (<U> <V> <O>)
	       (<S> <P> <O2>)
	       (<Q> <R> <O>)))

(define r (run* (q)
           (fresh (o delta) 
             (== q `(,delta ,o))
             (triple-nol delta '<Q> '<R> o)
             (triple-nol delta '<S> '<P> o)
             (triple-nol delta '<U> '<V> o))))

(print r)

(delete-triples '((<S> <P> <O>)))

(print (future r))

(add-triples '((<S> <P> <O3>)
               (<S> <P> <O>)
	       (<Q> <R> <O3>)
	       (<U> <V> <O3>)
	       (<S> <P> <M>)
	       (<U> <V> <M>)
	       (<Q> <R> <M>)))

(print (future (future r)))
