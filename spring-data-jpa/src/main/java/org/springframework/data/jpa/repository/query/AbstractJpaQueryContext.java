package org.springframework.data.jpa.repository.query;

import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import jakarta.persistence.QueryHint;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.ConfigurableConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.jpa.provider.PersistenceProvider;
import org.springframework.data.jpa.util.JpaMetamodel;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

abstract class AbstractJpaQueryContext implements QueryContext {

	static final ConversionService CONVERSION_SERVICE;

	static {

		ConfigurableConversionService conversionService = new DefaultConversionService();

		conversionService.addConverter(JpaResultConverters.BlobToByteArrayConverter.INSTANCE);
		conversionService.removeConvertible(Collection.class, Object.class);
		conversionService.removeConvertible(Object.class, Optional.class);

		CONVERSION_SERVICE = conversionService;
	}

	private final JpaQueryMethod method;
	private final EntityManager entityManager;
	private final JpaMetamodel metamodel;
	private final PersistenceProvider provider;

	protected ParameterBinder parameterBinder;

	public AbstractJpaQueryContext(JpaQueryMethod method, EntityManager entityManager, JpaMetamodel metamodel,
			PersistenceProvider provider) {

		this.method = method;
		this.entityManager = entityManager;
		this.metamodel = metamodel;
		this.provider = provider;
		this.parameterBinder = ParameterBinderFactory.createBinder(method.getParameters());
	}

	@Override
	public QueryMethod getQueryMethod() {
		return this.method;
	}

	public EntityManager getEntityManager() {
		return entityManager;
	}

	@Nullable
	@Override
	final public Object execute(Object[] parameters) {

		// create query
		String initialQuery = createQuery();

		// post-process query
		String processedQuery = processQuery(initialQuery);

		// parse query
		Query parsedQuery = createJpaQuery(processedQuery);

		// apply query hints
		Query parsedQueryWithHints = applyQueryHints(parsedQuery);

		Query parsedQueryWithHintsAndLockMode = applyLockMode(parsedQueryWithHints);

		// gather parameters and bind them to the query
		JpaParametersParameterAccessor accessor = obtainParameterAccessor(parameters);
		Query queryToExecute = bind(parsedQueryWithHintsAndLockMode, accessor);

		// execute query
		// JpaQueryContextExecution execution;
		//
		// if (method.isStreamQuery()) {
		// execution = new StreamExecution();
		// } else if (method.isProcedureQuery()) {
		// execution = new ProcedureExecution();
		// } else if (method.isCollectionQuery()) {
		// execution = new CollectionExecution();
		// } else if (method.isSliceQuery()) {
		// execution = new SlicedExecution();
		// } else if (method.isPageQuery()) {
		// execution = new PagedExecution();
		// } else if (method.isModifyingQuery()) {
		// execution = new ModifyingExecution(entityManager);
		// } else {
		// execution = new SingleEntityExecution();
		// }
		//
		// Object result = execution.execute(method, queryToExecute, accessor);
		Object result = execute(method, queryToExecute, accessor);

		// Unwrap results
		Object unwrapped = unwrapAndApplyProjections(result, accessor);

		// return results
		return unwrapped;
	}

	protected abstract String createQuery();

	/**
	 * By default, apply no processing and simply return the query unaltered.
	 *
	 * @param query
	 * @return modified query
	 */
	protected String processQuery(String query) {
		return query;
	}

	/**
	 * Transform a string query into a JPA {@link Query}.
	 *
	 * @param query
	 * @return
	 */
	protected Query createJpaQuery(String query) {
		return entityManager.createQuery(query, method.getReturnType());
	}

	protected Query applyQueryHints(Query query) {

		List<QueryHint> hints = method.getHints();

		if (!hints.isEmpty()) {
			for (QueryHint hint : hints) {
				applyQueryHint(query, hint);
			}
		}

		// Apply any meta-attributes that exist
		if (method.hasQueryMetaAttributes()) {

			if (provider.getCommentHintKey() != null) {
				query.setHint( //
						provider.getCommentHintKey(), provider.getCommentHintValue(method.getQueryMetaAttributes().getComment()));
			}
		}

		return query;
	}

	protected void applyQueryHint(Query query, QueryHint hint) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(hint, "QueryHint must not be null");

		query.setHint(hint.name(), hint.value());
	}

	protected Query applyLockMode(Query query) {

		LockModeType lockModeType = method.getLockModeType();
		return lockModeType == null ? query : query.setLockMode(lockModeType);
	}

	protected abstract Query bind(Query query, JpaParametersParameterAccessor accessor);

	protected Object execute(JpaQueryMethod method, Query queryToExecute, JpaParametersParameterAccessor accessor) {

		Assert.notNull(method, "JpaQueryMethod must not be null");
		Assert.notNull(queryToExecute, "Query must not be null");
		Assert.notNull(accessor, "JpaParametersParameterAccessor must not be null");

		Object result;

		try {
			result = doExecute(method, queryToExecute, accessor);
		} catch (NoResultException ex) {
			return null;
		}

		if (result == null) {
			return null;
		}

		Class<?> requiredType = method.getReturnType();

		if (ClassUtils.isAssignable(requiredType, void.class) || ClassUtils.isAssignableValue(requiredType, result)) {
			return result;
		}

		return CONVERSION_SERVICE.canConvert(result.getClass(), requiredType) //
				? CONVERSION_SERVICE.convert(result, requiredType) //
				: result;

	}

	protected abstract Object doExecute(JpaQueryMethod method, Query queryToExecute,
			JpaParametersParameterAccessor accessor);

	protected Object unwrapAndApplyProjections(Object result, JpaParametersParameterAccessor accessor) {

		ResultProcessor withDynamicProjection = method.getResultProcessor().withDynamicProjection(accessor);

		// TODO: Migrate TupleConverter to its own class?
		return withDynamicProjection.processResult(result,
				new AbstractJpaQuery.TupleConverter(withDynamicProjection.getReturnedType()));
	}

	private JpaParametersParameterAccessor obtainParameterAccessor(Object[] values) {

		if (method.isNativeQuery() && PersistenceProvider.HIBERNATE.equals(provider)) {
			return new HibernateJpaParametersParameterAccessor(method.getParameters(), values, entityManager);
		}

		return new JpaParametersParameterAccessor(method.getParameters(), values);
	}

	/**
	 * Base class that defines how queries are executed using JPA.
	 */
	private abstract class JpaQueryContextExecution {

		static final ConversionService CONVERSION_SERVICE;

		static {

			ConfigurableConversionService conversionService = new DefaultConversionService();

			conversionService.addConverter(JpaResultConverters.BlobToByteArrayConverter.INSTANCE);
			conversionService.removeConvertible(Collection.class, Object.class);
			conversionService.removeConvertible(Object.class, Optional.class);

			CONVERSION_SERVICE = conversionService;
		}

		@Nullable
		public Object execute(JpaQueryMethod queryMethod, Query query, JpaParametersParameterAccessor accessor) {

			Assert.notNull(queryMethod, "JpaQueryMethod must not be null");
			Assert.notNull(query, "Query must not be null");
			Assert.notNull(accessor, "JpaParametersParameterAccessor must not be null");

			Object result;

			try {
				result = doExecute(queryMethod, query, accessor);
			} catch (NoResultException ex) {
				return null;
			}

			if (result == null) {
				return null;
			}

			Class<?> requiredType = queryMethod.getReturnType();

			if (ClassUtils.isAssignable(requiredType, void.class) || ClassUtils.isAssignableValue(requiredType, result)) {
				return result;
			}

			return CONVERSION_SERVICE.canConvert(result.getClass(), requiredType) //
					? CONVERSION_SERVICE.convert(result, requiredType) //
					: result;
		}

		@Nullable
		protected abstract Object doExecute(JpaQueryMethod queryMethod, Query query,
				JpaParametersParameterAccessor accessor);
	}

	// /**
	// * Execute a JPA query for a Java 8 {@link java.util.stream.Stream}-returning repository method.
	// */
	// private class StreamExecution extends JpaQueryContextExecution {
	//
	// private static final String NO_SURROUNDING_TRANSACTION = "You're trying to execute a streaming query method without
	// a surrounding transaction that keeps the connection open so that the Stream can actually be consumed; Make sure the
	// code consuming the stream uses @Transactional or any other way of declaring a (read-only) transaction";
	//
	// @Override
	// protected Object doExecute(JpaQueryMethod queryMethod, Query query, JpaParametersParameterAccessor accessor) {
	//
	// if (!SurroundingTransactionDetectorMethodInterceptor.INSTANCE.isSurroundingTransactionActive()) {
	// throw new InvalidDataAccessApiUsageException(NO_SURROUNDING_TRANSACTION);
	// }
	//
	// return query.getResultStream();
	// }
	// }
	//
	// /**
	// * Execute a JPA stored procedure.
	// */
	// private class ProcedureExecution extends JpaQueryContextExecution {
	//
	// private static final String NO_SURROUNDING_TRANSACTION = "You're trying to execute a @Procedure method without a
	// surrounding transaction that keeps the connection open so that the ResultSet can actually be consumed; Make sure
	// the consumer code uses @Transactional or any other way of declaring a (read-only) transaction";
	//
	// @Override
	// protected Object doExecute(JpaQueryMethod queryMethod, Query query, JpaParametersParameterAccessor accessor) {
	//
	// Assert.isInstanceOf(StoredProcedureQuery.class, query);
	//
	// StoredProcedureQuery procedure = (StoredProcedureQuery) query;
	//
	// try {
	// boolean returnsResultSet = procedure.execute();
	//
	// if (returnsResultSet) {
	//
	// if (!SurroundingTransactionDetectorMethodInterceptor.INSTANCE.isSurroundingTransactionActive()) {
	// throw new InvalidDataAccessApiUsageException(NO_SURROUNDING_TRANSACTION);
	// }
	//
	// return queryMethod.isCollectionQuery() ? procedure.getResultList() : procedure.getSingleResult();
	// }
	//
	// return extractOutputValue(procedure); // extract output value from the procedure
	// } finally {
	// if (procedure instanceof AutoCloseable autoCloseable) {
	// try {
	// autoCloseable.close();
	// } catch (Exception ignored) {}
	// }
	// }
	// }
	// }
	//
	// /**
	// * Execute a JPA query for a Java {@link Collection}-returning repository method.
	// */
	// private class CollectionExecution extends JpaQueryContextExecution {
	//
	// @Override
	// protected Object doExecute(JpaQueryMethod queryMethod, Query query, JpaParametersParameterAccessor accessor) {
	// return query.getResultList();
	// }
	// }
	//
	// /**
	// * Execute a JPA query for a Spring Data {@link org.springframework.data.domain.Slice}-returning repository method.
	// */
	// private class SlicedExecution extends JpaQueryContextExecution {
	//
	// @Override
	// protected Object doExecute(JpaQueryMethod queryMethod, Query query, JpaParametersParameterAccessor accessor) {
	//
	// Pageable pageable = accessor.getPageable();
	//
	// int pageSize = 0;
	// if (pageable.isPaged()) {
	//
	// pageSize = pageable.getPageSize();
	// query.setMaxResults(pageSize + 1);
	// }
	//
	// List<?> resultList = query.getResultList();
	//
	// boolean hasNext = pageable.isPaged() && resultList.size() > pageSize;
	//
	// if (hasNext) {
	// return new SliceImpl<>(resultList.subList(0, pageSize), pageable, hasNext);
	// } else {
	// return new SliceImpl<>(resultList, pageable, hasNext);
	// }
	// }
	// }
	//
	// /**
	// * Execute a JPA query for a Spring Data {@link org.springframework.data.domain.Page}-returning repository method.
	// */
	// private class PagedExecution extends JpaQueryContextExecution {
	//
	// @Override
	// protected Object doExecute(JpaQueryMethod queryMethod, Query query, JpaParametersParameterAccessor accessor) {
	// return PageableExecutionUtils.getPage(query.getResultList(), accessor.getPageable(),
	// () -> count(queryMethod, query, accessor));
	// }
	//
	// private long count(JpaQueryMethod queryMethod, Query query, JpaParametersParameterAccessor accessor) {
	//
	// List<?> totals = query.getResultList();
	// return totals.size() == 1 //
	// ? CONVERSION_SERVICE.convert(totals.get(0), Long.class) //
	// : totals.size();
	// }
	// }
	//
	// /**
	// * Execute a JPA query for a repository with an @{@link org.springframework.data.jpa.repository.Modifying}
	// annotation
	// * applied.
	// */
	// private class ModifyingExecution extends JpaQueryContextExecution {
	//
	// private final EntityManager entityManager;
	//
	// public ModifyingExecution(EntityManager entityManager) {
	//
	// Assert.notNull(entityManager, "EntityManager must not be null");
	//
	// this.entityManager = entityManager;
	// }
	//
	// @Override
	// protected Object doExecute(JpaQueryMethod queryMethod, Query query, JpaParametersParameterAccessor accessor) {
	//
	// Class<?> returnType = method.getReturnType();
	//
	// boolean isVoid = ClassUtils.isAssignable(returnType, Void.class);
	// boolean isInt = ClassUtils.isAssignable(returnType, Integer.class);
	//
	// Assert.isTrue(isInt || isVoid,
	// "Modifying queries can only use void or int/Integer as return type; Offending method: " + method);
	//
	// if (method.getFlushAutomatically()) {
	// entityManager.flush();
	// }
	//
	// int result = query.executeUpdate();
	//
	// if (method.getClearAutomatically()) {
	// entityManager.clear();
	// }
	//
	// return result;
	// }
	// }
	//
	// /**
	// * Execute a JPA query for a repository method returning a single value.
	// */
	// private class SingleEntityExecution extends JpaQueryContextExecution {
	//
	// @Override
	// protected Object doExecute(JpaQueryMethod queryMethod, Query query, JpaParametersParameterAccessor accessor) {
	// return query.getSingleResult();
	// }
	// }
}