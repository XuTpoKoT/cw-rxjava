package org.example;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class RxTests {

    private static Observable<Integer> range(int start, int count) {
        return Observable.<Integer>create(emitter -> {
            for (int i = 0; i < count; i++) {
                emitter.onNext(start + i);
            }
            emitter.onComplete();
        });
    }

    @Test
    void testBasicSubscription() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        List<Integer> results = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        }).subscribe(new Observer<Integer>() {
            @Override
            public void onNext(Integer i) {
                results.add(i);
            }

            @Override
            public void onError(Throwable t) {
                fail("Unexpected error: " + t.getMessage());
            }

            @Override
            public void onComplete() {
                completed.set(true);
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(List.of(1, 2, 3), results);
        assertTrue(completed.get());
    }

    @Test
    void testMapAndFilter() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        List<Integer> results = new ArrayList<>();

        range(1, 10)
                .filter(x -> x % 2 == 0)
                .map(x -> x * 2)
                .subscribe(new Observer<Integer>() {
                    @Override
                    public void onNext(Integer i) {
                        results.add(i);
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("Unexpected error");
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(List.of(4, 8, 12, 16, 20), results);
    }

    @Test
    void testFlatMap() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        List<String> results = new ArrayList<>();

        range(1, 3)
                .flatMap(x -> Observable.<String>create(e -> {
                    e.onNext("A" + x);
                    e.onNext("B" + x);
                    e.onComplete();
                }))
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String s) {
                        results.add(s);
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("Unexpected error");
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(List.of("A1", "B1", "A2", "B2", "A3", "B3"), results);
    }

    @Test
    void testErrorPropagation() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean errorReceived = new AtomicBoolean(false);

        Observable.<Object>create(emitter -> emitter.onError(new RuntimeException("boom")))
                .subscribe(new Observer<Object>() {
                    @Override
                    public void onNext(Object o) {
                        fail("Should not receive onNext");
                    }

                    @Override
                    public void onError(Throwable t) {
                        assertEquals("boom", t.getMessage());
                        errorReceived.set(true);
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        fail("Should not complete");
                    }
                });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(errorReceived.get());
    }

    @Test
    void testSubscribeOnObserveOn() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        List<String> threads = new ArrayList<>();

        var io = new IOThreadScheduler();
        var single = new SingleThreadScheduler();

        Observable.<Integer>create(emitter -> {
                    threads.add("emit-" + Thread.currentThread().getName());
                    emitter.onNext(1);
                    emitter.onComplete();
                })
                .subscribeOn(io)
                .observeOn(single)
                .map(x -> {
                    threads.add("map-" + Thread.currentThread().getName());
                    return "value-" + x;
                })
                .subscribe(new Observer<String>() {
                    @Override
                    public void onNext(String s) {
                        threads.add("onNext-" + Thread.currentThread().getName());
                    }

                    @Override
                    public void onError(Throwable t) {
                        fail("Unexpected error");
                    }

                    @Override
                    public void onComplete() {
                        threads.add("complete-" + Thread.currentThread().getName());
                        latch.countDown();
                    }
                });

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        boolean emitInIO = threads.stream().anyMatch(t -> t.startsWith("emit-") && t.contains("pool"));
        boolean onNextInSingle = threads.stream().anyMatch(t -> t.startsWith("onNext-") && t.contains("single"));

        io.shutdown();
        single.shutdown();
    }

    @Test
    void testDisposable() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean received = new AtomicBoolean(false);

        Disposable d = Observable.<Integer>create(emitter -> {
            new Thread(() -> {
                try {
                    Thread.sleep(100);
                    emitter.onNext(1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
            emitter.onComplete();
        }).subscribe(new Observer<Integer>() {
            @Override
            public void onNext(Integer i) {
                received.set(true);
            }

            @Override
            public void onError(Throwable t) {
                // Игнорируем ошибки
            }

            @Override
            public void onComplete() {
                latch.countDown();
            }
        });

        Thread.sleep(50);
        d.dispose();
        Thread.sleep(150);

        latch.countDown();

        assertFalse(received.get(), "onNext не должен был сработать после dispose()");
    }
}
