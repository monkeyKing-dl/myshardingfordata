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
		log.debug("begin transaction  {}", Thread.currentThread().getName());
		try {
			boolean b = connectionManager.beginTransaction(getReadOnly(pjp));
			Object rz = pjp.proceed();
			if (b) {
				log.debug("commit transaction  {}", Thread.currentThread().getName());
				connectionManager.commitTransaction();
			}
			return rz;
		} catch (Throwable e) {
			log.debug("rollback transaction  {}", Thread.currentThread().getName());
			connectionManager.rollbackTransaction();
			throw e;
		}

	}

	private boolean getReadOnly(ProceedingJoinPoint pjp) throws NoSuchMethodException {
		boolean readOnly = false;
		MethodSignature signature = (MethodSignature) pjp.getSignature();
		Method method = signature.getMethod();
		MyTransaction myAnnotation = method.getAnnotation(MyTransaction.class);
		if (myAnnotation != null) {
			// cglib
			readOnly = myAnnotation.readOnly();
			log.debug("proxyTargetClass:{}", true);
		} else {
			// jdk PROXY
			Method tgmethod = pjp.getTarget().getClass().getMethod(method.getName(), method.getParameterTypes());
			if (tgmethod != null) {
				myAnnotation = tgmethod.getAnnotation(MyTransaction.class);
				readOnly = myAnnotation.readOnly();
				log.debug("proxyTargetClass:{}", false);
			} else {
				log.error("target method  is null ");
			}
		}
		log.debug("readOnly:{}", readOnly);
		return readOnly;
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
