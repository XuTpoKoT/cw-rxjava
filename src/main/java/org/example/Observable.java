package org.example;

import java.util.function.Function;
import java.util.function.Predicate;

public class Observable<T> {

    private final Emitter<T> emitterFactory;
    private Scheduler subscribeScheduler = null;
    private Scheduler observeScheduler = null;

    private Observable(Emitter<T> factory) {
        this.emitterFactory = factory;
    }

    public static <T> Observable<T> create(Emitter<T> emitter) {
        return new Observable<>(emitter);
    }

    public Disposable subscribe(Observer<T> observer) {
        // Если есть subscribeOn, оборачиваем фабрику эмиттера, чтобы emit() выполнялся в нужном потоке
        Emitter<T> actualEmitter;
        if (subscribeScheduler != null) {
            actualEmitter = e -> subscribeScheduler.execute(() -> emitterFactory.emit(e));
        } else {
            actualEmitter = emitterFactory;
        }

        // Создаем обертку, которая при вызове onNext/onError/onComplete от источника
        // может перенаправить выполнение в observeOn-поток
        EmitterWrapper<T> wrapper = new EmitterWrapper<>(observer, observeScheduler);

        actualEmitter.emit(wrapper);
        return wrapper;
    }

    public Observable<T> subscribeOn(Scheduler scheduler) {
        Observable<T> copy = new Observable<>(this.emitterFactory);
        copy.subscribeScheduler = scheduler;
        copy.observeScheduler = this.observeScheduler; // сохраняем observeOn, если был
        return copy;
    }

    public Observable<T> observeOn(Scheduler scheduler) {
        Observable<T> copy = new Observable<>(this.emitterFactory);
        copy.observeScheduler = scheduler;
        copy.subscribeScheduler = this.subscribeScheduler; // сохраняем subscribeOn, если был
        return copy;
    }

    public <R> Observable<R> map(Function<T, R> mapper) {
        return Observable.create(emitter -> this.subscribe(new Observer<T>() {
            @Override
            public void onNext(T t) {
                try {
                    R result = mapper.apply(t);
                    emitter.onNext(result);
                } catch (Throwable e) {
                    emitter.onError(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                emitter.onError(t);
            }

            @Override
            public void onComplete() {
                emitter.onComplete();
            }
        }));
    }

    public Observable<T> filter(Predicate<T> predicate) {
        return Observable.create(emitter -> this.subscribe(new Observer<T>() {
            @Override
            public void onNext(T t) {
                if (predicate.test(t)) {
                    emitter.onNext(t);
                }
            }

            @Override
            public void onError(Throwable t) {
                emitter.onError(t);
            }

            @Override
            public void onComplete() {
                emitter.onComplete();
            }
        }));
    }

    public <R> Observable<R> flatMap(Function<T, Observable<R>> mapper) {
        return Observable.create(emitter -> {
            this.subscribe(new Observer<T>() {
                @Override
                public void onNext(T t) {
                    try {
                        Observable<R> inner = mapper.apply(t);
                        inner.subscribe(new Observer<R>() {
                            @Override
                            public void onNext(R r) {
                                emitter.onNext(r);
                            }

                            @Override
                            public void onError(Throwable e) {
                                emitter.onError(e);
                            }

                            @Override
                            public void onComplete() {
                                // Внутреннее завершение не завершает внешний поток
                            }
                        });
                    } catch (Throwable e) {
                        emitter.onError(e);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    emitter.onError(t);
                }

                @Override
                public void onComplete() {
                    emitter.onComplete();
                }
            });
        });
    }

    @FunctionalInterface
    public interface Emitter<T> {
        void emit(InnerEmitter<T> e);
    }

    public interface InnerEmitter<T> {
        void onNext(T value);
        void onError(Throwable t);
        void onComplete();
    }

    private static class EmitterWrapper<T> implements InnerEmitter<T>, Disposable {
        private final Observer<T> observer;
        private final Scheduler observeScheduler;
        private volatile boolean disposed = false;

        public EmitterWrapper(Observer<T> observer, Scheduler observeScheduler) {
            this.observer = observer;
            this.observeScheduler = observeScheduler;
        }

        @Override
        public void onNext(T value) {
            if (disposed) return;
            if (observeScheduler != null) {
                observeScheduler.execute(() -> observer.onNext(value));
            } else {
                observer.onNext(value);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (disposed) return;
            disposed = true;
            if (observeScheduler != null) {
                observeScheduler.execute(() -> observer.onError(t));
            } else {
                observer.onError(t);
            }
        }

        @Override
        public void onComplete() {
            if (disposed) return;
            disposed = true;
            if (observeScheduler != null) {
                observeScheduler.execute(() -> observer.onComplete());
            } else {
                observer.onComplete();
            }
        }

        @Override
        public void dispose() {
            disposed = true;
        }

        @Override
        public boolean isDisposed() {
            return disposed;
        }
    }
}
