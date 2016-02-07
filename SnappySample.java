public class Main {

    public static void main(String[] args) {


        try {
            DB snappydb = DBFactory.open(context);

            //Get/Put/Del
            snappydb.put("name", "Jack Reacher");
            String name =  snappydb.get("name");
            snappyDB.del("name");

            //Key search
            String [] keys = snappyDB.findKeys("android");

            //Iteration
            it = snappyDB.allKeysIterator();

            snappydb.close();

        } catch (SnappydbException e) {
        }


        SnappyrDB snappyrdb = new SnappyrDB(context);

        snappyrdb.query()
        .put("Key1", "Value1")
        .del("Key1")
        .get("Key1", String.class)
        .subscribe(
            (s) -> System.out.println(s), 
            (e) -> e.printStackTrace(), 
            () -> System.out.println("Demo query executed"));


    }
}

        public class SnappyrQuery {

            private Observable<DB> dbObs;

            public SnappyrQuery(DB dbObs) {
                if(dbObs != null) {
                    this.dbObs = Observable.just(dbObs);
                }
                else {
                    this.dbObs = Observable.error(
                        new NullPointerException("Missing DB"));
                }
            }

            ..



    public <T> Observable<T> lift(Observable.Operator<T, DB> operator) {
        return dbObs.lift(operator);
    }

    public SnappyrQuery query(Observable.Operator<DB, DB> operator) {
        return new SnappyrQuery(this.lift(operator));
    }

    public <T> SnappyrQuery put(String key, T value) {
        return query(new Put(key, value));
    }

    public SnappyrQuery del(String key) {
        return query(new Delete(key));
    }






public class Put implements Operator<DB, DB> {
    ..

    public Subscriber<? super DB> call(
        final Subscriber<? super DB> s) {

        return new Subscriber<DB>(s) {
            @Override
            public void onCompleted() {
                if(!s.isUnsubscribed()) {
                    s.onCompleted();
                }
            }

            @Override
            public void onError(Throwable t) {
                if(!s.isUnsubscribed()) {
                    s.onError(t);
                }
            }

            @Override
            public void onNext(DB item) {
                if(!s.isUnsubscribed()) {
                    try {
                        ByteArrayOutputStream stream = 
                            new ByteArrayOutputStream();
                        Output output = new Output(stream);
                        kryo.writeObject(output, value);
                        output.close();
                        item.put(bytes(key), stream.toByteArray());
                        s.onNext(item);
                    }
                    catch(Exception e) {
                        s.onError(e);
                    }
                }
            }
        };
    }
}


    public Subscription subscribe(
        final Action1<? super DB> onNext, 
        final Action1<Throwable> onError, 
        final Action0 onComplete) {

        return dbObs.subscribe(onNext, onError, onComplete);
    }




public class Get implements Operator<Observable<Map.Entry<String, byte[]>>, DB> {
    ..

    public Subscriber<? super DB> call(
        final Subscriber<? super Observable<Map.Entry<String, byte[]>>> s) {
        ..

        public void onNext(DB db) {
            s.onNext(
                Observable.create(new OnSubscribeFromSnappyDb(db))
                .flatMap(new Func1<Map.Entry<byte[], byte[]>, Observable<Map.Entry<String, byte[]>>>() {
                    //Filter and convert
                    @Override
                    public Observable<Map.Entry<String, byte[]>> call(Map.Entry<byte[], byte[]> e) {
                        String key = new String(e.getKey());
                        if(keyPred.call(key)) {
                            return Observable.just(
                                (Map.Entry<String, byte[]>) new AbstractMap.SimpleEntry<>(key, e.getValue()));
                        }
                        else {
                            return Observable.empty();
                        }
                    }
                })
            );
        }
    }









        snappyrdb.query()
        .subscribeOn(Schedulers.newThread())
        .put("Key2", "Value2")
        .doOnNext((db) ->  
            System.out.println("Thread # " + Thread.currentThread().getId()))
        .observeOn(Schedulers.newThread())
        .doOnNext((db) ->  
            System.out.println("Thread # " + Thread.currentThread().getId()))
        .put("Key2", "Value2")
        .subscribe()



        snappyrdb.query()
        .get((s) -> s.startsWith("Key"), String.class)
        .lift(new AssignKey<String>((k) -> k + "_updated")
        .extend(new PutIn<String>(snappyrdb))
        .subscribe();









public class PutIn <T> implements Func1<Observable.OnSubscribe<Map.Entry<String, T>>, SnappyrQuery> {
    SnappyrDB db;

    public PutIn(SnappyrDB db) {
        this.db = db;
    }

    @Override
    public SnappyrQuery call(final Observable.OnSubscribe<Map.Entry<String, T>> entryOnSubscribe) {

        final ReplaySubject<DB> subj = ReplaySubject.create();
        final SnappyrQuery query = new SnappyrQuery(subj);

        entryOnSubscribe.call(new Subscriber<Map.Entry<String, T>>() {
            final Subscriber<Map.Entry<String, T>> subscriber = this;

            @Override
            public void onCompleted() {
                subj.onNext(db.getDb());
                subj.onCompleted();
            }

            @Override
            public void onError(Throwable throwable) {
                subj.onError(throwable);
                subscriber.unsubscribe();
            }

            @Override
            public void onNext(Map.Entry<String, T> stringEntry) {
                 query.put(stringEntry.getKey(), stringEntry.getValue())
                .subscribe(new Action1<Throwable>() {
                    @Override
                    public void call(Throwable e) {
                        subj.onError(e);
                        subscriber.unsubscribe();
                    }
                }, new Action0() {
                    @Override
                    public void call() {
                        //Do nothing on completed of single lift
                    }
                });
            }
        });
        return query;
    }
}


snappyrdb.query()
.getKey((s) -> true)
.extend(new DeleteFrom(snappyrdb))
.subscribe();






SnappyDB
//Get value from SnappyDB and perform action with it
.get("Key1", a1)                                                            ;
//Put a new value in the database   
.put("Key1", val1)                                                          ;
//Delete a key
.del("Key1");



SnappyDB
//Get value from SnappyDB and perform action with it
.get("Key1")                                                                    ;
.doOnNext((s) -> {...} ) //Do actions
.map((s) -> s + "_updated")                                                  ;
//Put the updated value in the database   
.put("Key1")                                                                    ;
//Delete a key
.del("Key1");



