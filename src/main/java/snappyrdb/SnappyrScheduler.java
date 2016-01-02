package snappyrdb;
import java.util.concurrent.TimeUnit;

import javafx.application.Platform;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;

public class SnappyrScheduler extends Scheduler {
    private static SnappyrScheduler instance;

    private SnappyrScheduler(){}

    public static SnappyrScheduler getInstance(){
        if(instance == null){
            instance = new SnappyrScheduler();
        }
        return instance;
    }

    @Override
    public Worker createWorker() {
        return new SnappyrWorker();
    }

    public class SnappyrWorker extends Worker{

        private Subscription subscription;

        public SnappyrWorker() {
            this.subscription = new Subscription() {

                private boolean isUnsubscribed = false;

                @Override
                public void unsubscribe() {
                    isUnsubscribed = true;
                }

                @Override
                public boolean isUnsubscribed() {
                    return isUnsubscribed;
                }
            };
        }

        @Override
        public void unsubscribe() {
            subscription.unsubscribe();
        }

        @Override
        public boolean isUnsubscribed() {
            return subscription.isUnsubscribed();
        }

        @Override
        public Subscription schedule(final Action0 action) {
            Platform.runLater(new Runnable() {
                @Override
                public void run() {
                    if(!isUnsubscribed()){
                        action.call();
                    }
                }
            });
            return this;
        }

        @Override
        public Subscription schedule(final Action0 action, final long delayTime, final TimeUnit unit) {
            Platform.runLater(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(unit.toMillis(delayTime));
                        if(!isUnsubscribed()){
                            action.call();
                        }
                    } catch (Exception e) {
                    }
                }
            });
            return this;
        }
    }
}