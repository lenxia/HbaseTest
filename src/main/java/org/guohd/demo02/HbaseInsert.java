package org.guohd.demo02;

import java.io.IOException;

public class HbaseInsert {
	/*
	 * 单线程插入数据
	 * 
	 */
	public static void SingleThreadInsert(String tableName, int count)
			throws IOException {
		System.out.println("---------开始SingleThreadInsert测试----------");
		long start = System.currentTimeMillis();

		HbaseUtils.insert(tableName,count);

		long stop = System.currentTimeMillis();

		System.out.println("插入数据：" + count + "共耗时：" + (stop - start) * 1.0
				/ 1000 + "s");

		System.out.println("---------结束SingleThreadInsert测试----------");

	}

	/*
	 * 多线程插入数据
	 * 
	 */
	public static void MultThreadInsert(String tableName, int count)
			throws IOException {
		System.out.println("---------开始MultThreadInsert测试----------");
		long start = System.currentTimeMillis();

		// 同事开启10个线程写入数据
		int threadNumber = 10;
		Thread[] threads = new Thread[threadNumber];
		for (int i = 0; i < threads.length; i++) {
			threads[i] = new ImportThread();
			threads[i].start();
		}

		long stop = System.currentTimeMillis();

		System.out.println("MultThreadInsert：" + threadNumber * count + "共耗时："
				+ (stop - start) * 1.0 / 1000 + "s");
		System.out.println("---------结束MultThreadInsert测试----------");

	}

	public HbaseInsert() {
		super();
	}

}