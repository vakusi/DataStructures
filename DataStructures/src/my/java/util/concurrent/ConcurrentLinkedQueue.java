package my.java.util.concurrent;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * My effor on building a simple ConcurrentLinkedQueue implementation.
 * @author Vashistha Singh
 *
 * @param <T>
 */
public class ConcurrentLinkedQueue<T> implements Iterable{

	final AtomicReference<Node<T>> head;
	final AtomicReference<Node<T>> tail;
	final AtomicLong sequenceId = new AtomicLong(0);
	ConcurrentLinkedQueue() {
		head = new AtomicReference<Node<T>>();
		tail = new AtomicReference<Node<T>>();
	}

	static class Node<T> {
		final AtomicReference<Node<T>> next = new AtomicReference<Node<T>>();
		T data;
		long sequenceid;
		Node(T data) {
			this.data = data;
		}
	}

	/**Non blocking offer method. 
	 * @param o
	 * @return
	 */
	public boolean offer(T o) {
		boolean result = false;
		Node<T> newNode = new Node<T>(o);
		while (true) {
			Node<T> prev = tail.get();
			if (prev == null) {
				if (tail.compareAndSet(null, newNode)) {
					newNode.sequenceid = sequenceId.getAndIncrement();
					if (head.compareAndSet(null, newNode)) {
						result = true;
						break;
					} else {
						// This should never happen
						throw new RuntimeException("This is breakage!!!");
					}
				}
			} else {
				if (prev.next.compareAndSet(null, newNode)) {
					newNode.sequenceid = sequenceId.getAndIncrement();
					tail.compareAndSet(prev, newNode);
					result = true;
					break;
				}
			}
		}
		return result;
	}

	/**Non-blocking poll method.
	 * @return
	 */
	public T poll() {
		T result = null;
		if (head.get() == null) {
			return null;
		}
		while (true) {
			Node<T> prev = head.get();
			if (head.compareAndSet(prev, prev.next.get())) {
				prev.next.set(null);
				result = prev.data;
				break;
			}
		}
		return result;
	}

	/**
	 * Test program
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String args[]) throws InterruptedException {
		ConcurrentLinkedQueue<Long> myQueue = new ConcurrentLinkedQueue<Long>();
		final class SimpleRunnable implements Runnable {
			final AtomicLong data;
			final ConcurrentLinkedQueue<Long> queue;
			final CountDownLatch latch;
			public SimpleRunnable(AtomicLong newData,
					ConcurrentLinkedQueue<Long> newQueue, CountDownLatch newLatch) {
				data = newData;
				queue = newQueue;
				latch = newLatch;
			}

			@Override
			public void run() {
				int counter = 1000;
				while(counter-->0)
				{
					long result = data.getAndIncrement();
					queue.offer(result);
					System.out.println(Thread.currentThread().getName()+ "Adding: "+result);
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
					}
				}
				latch.countDown();
			}
		}
		AtomicLong data = new AtomicLong(0);
		ExecutorService executors = Executors.newFixedThreadPool(10);
		CountDownLatch latch = new CountDownLatch(10);
		for (int i = 0; i < 10; i++) {
			executors.execute(new SimpleRunnable(data, myQueue,latch));
		}
		latch.await();
		executors.shutdown();

		Iterator<Node<Long>> itr = myQueue.iterator();
		while(itr.hasNext())
		{
			System.out.println(itr.next().sequenceid);
		}
		
	}
	@Override
	public Iterator<Node<T>> iterator() {
		
		return new MyIterator<T>();
	}
	
	/**
	 * Iterator for this collection.
	 *
	 * @param <T>
	 */
	class MyIterator<T> implements Iterator
	{
		Node<T> current = (Node<T>) head.get();
		@Override
		public boolean hasNext() {
			return current!=null && tail.get().next.get()!=current;
		}

		@Override
		public Node<T> next() {
			Node<T> result = current;
			current = current.next.get();
			return result;
		}
		
	}
}
