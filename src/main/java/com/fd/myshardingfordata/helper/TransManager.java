package com.fd.myshardingfordata.helper;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fd.myshardingfordata.annotation.MyTransaction;

/**
 * 事物管理器
 * 
 * @author 符冬
 *
 */
@Aspect
public class TransManager {
	@Around("@annotation(com.fd.myshardingfordata.annotation.MyTransaction)")
	public Object transactional(ProceedingJoinPoint pjp) throws Throwable {
		log.debug("begin transaction  {}",Thread.currentThread().getName());
		Object rz = null;
		boolean b;
		try {
			MethodSignature signature = (MethodSignature) pjp.getSignature();
			Method method = signature.getMethod();
			MyTransaction myAnnotation = method.getAnnotation(MyTransaction.class);

			b = connectionManager.beginTransaction(myAnnotation.readOnly());
			rz = pjp.proceed();
			if (b) {
				log.debug("commit transaction  {}",Thread.currentThread().getName());
				connectionManager.commitTransaction();
			}
		} catch (Throwable e) {
			log.debug("rollback transaction  {}",Thread.currentThread().getName());
			connectionManager.rollbackTransaction();
			throw e;
		}
		log.debug("end transaction  {}",Thread.currentThread().getName());
		return rz;
	}

	public void setConnectionManager(IConnectionManager connectionManager) {
		this.connectionManager = connectionManager;
	}

	public TransManager(IConnectionManager connectionManager) {
		super();
		this.connectionManager = connectionManager;
	}

	public TransManager() {
		super();
	}

	private static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	private IConnectionManager connectionManager;
}
