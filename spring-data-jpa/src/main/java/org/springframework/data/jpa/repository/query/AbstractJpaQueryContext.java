package org.springframework.data.jpa.repository.query;

import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Query;
import jakarta.persistence.QueryHint;
import jakarta.persistence.StoredProcedureQuery;
import jakarta.persistence.Tuple;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.ConfigurableConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.jpa.provider.PersistenceProvider;
import org.springframework.data.jpa.util.JpaMetamodel;
import org.springframework.data.repository.core.support.SurroundingTransactionDetectorMethodInterceptor;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.support.PageableExecutionUtils;
import org.springframework.data.util.Lazy;
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

	private final JpaQueryContextExecutor executor;

	protected final QueryParameterSetter.QueryMetadataCache metadataCache = new QueryParameterSetter.QueryMetadataCache();

	protected final Lazy<ParameterBinder> parameterBinder;

	public AbstractJpaQueryContext(JpaQueryMethod method, EntityManager entityManager, JpaMetamodel metamodel,
			PersistenceProvider provider) {

		this.method = method;
		this.entityManager = entityManager;
		this.metamodel = metamodel;
		this.provider = provider;
		this.parameterBinder = Lazy.of(this::createBinder);

		if (method.isStreamQuery()) {
			this.executor = new StreamExecutor(this);
		} else if (method.isProcedureQuery()) {
			this.executor = new ProcedureExecutor(this);
		} else if (method.isCollectionQuery()) {
			this.executor = new CollectionExecutor(this);
		} else if (method.isSliceQuery()) {
			this.executor = new SlicedExecutor(this);
		} else if (method.isPageQuery()) {
			this.executor = new PagedExecutor(this);
		} else if (method.isModifyingQuery()) {
			this.executor = new ModifyingExecutor(this, entityManager);
		} else {
			this.executor = new SingleEntityExecutor(this);
		}

	}

	protected ParameterBinder createBinder() {
		return ParameterBinderFactory.createBinder(method.getParameters());
	}

	@Override
	public JpaQueryMethod getQueryMethod() {
		return this.method;
	}

	public JpaMetamodel getMetamodel() {
		return metamodel;
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
		JpaParametersParameterAccessor accessor = obtainParameterAccessor(parameters);
		String processedQuery = processQuery(initialQuery, accessor);

		// parse query
		Query parsedQuery = createJpaQuery(processedQuery, accessor);

		// apply query hints
		Query parsedQueryWithHints = applyQueryHints(parsedQuery);

		// apply lock mode
		Query parsedQueryWithHintsAndLockMode = applyLockMode(parsedQueryWithHints);

		// gather parameters and bind them to the query
		Query queryToExecute = bind(parsedQueryWithHintsAndLockMode, accessor);

		// execute query
		Object rawResults = executor.execute(this, queryToExecute, accessor);

		// Unwrap results
		Object unwrappedResults = unwrapAndApplyProjections(rawResults, accessor);

		// return results
		return unwrappedResults;
	}

	/**
	 * Every form of query must produce a string-based query.
	 * 
	 * @return
	 */
	protected abstract String createQuery();

	/**
	 * By default, apply no processing and simply return the query unaltered.
	 *
	 * @param query
	 * @return modified query
	 */
	protected String processQuery(String query, JpaParametersParameterAccessor accessor) {
		return query;
	}

	/**
	 * Transform a string query into a JPA {@link Query}.
	 *
	 * @param query
	 * @return
	 */
	protected Query createJpaQuery(String query, JpaParametersParameterAccessor accessor) {
		return entityManager.createQuery(query, method.getReturnType());
	}

	protected abstract Query createCountQuery(JpaParametersParameterAccessor values);

	@Nullable
	protected Class<?> getTypeToRead(ReturnedType returnedType) {

		if (PersistenceProvider.ECLIPSELINK.equals(provider)) {
			return null;
		}

		return returnedType.isProjecting() && !getMetamodel().isJpaManaged(returnedType.getReturnedType()) //
				? Tuple.class //
				: null;
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

	protected Object unwrapAndApplyProjections(Object result, JpaParametersParameterAccessor accessor) {

		ResultProcessor withDynamicProjection = method.getResultProcessor().withDynamicProjection(accessor);

		// TODO: Migrate TupleConverter to its own class?
		return withDynamicProjection.processResult(result,
				new AbstractJpaQuery.TupleConverter(withDynamicProjection.getReturnedType()));
	}

	/**
	 * Transform the incoming array of arguments into a {@link JpaParametersParameterAccessor}.
	 * 
	 * @param values
	 * @return
	 */
	private JpaParametersParameterAccessor obtainParameterAccessor(Object[] values) {

		if (method.isNativeQuery() && PersistenceProvider.HIBERNATE.equals(provider)) {
			return new HibernateJpaParametersParameterAccessor(method.getParameters(), values, entityManager);
		}

		return new JpaParametersParameterAccessor(method.getParameters(), values);
	}

	/**
	 * Base class that defines how queries are executed with JPA.
	 */
	private abstract class JpaQueryContextExecutor {

		static final ConversionService CONVERSION_SERVICE;

		static {

			ConfigurableConversionService conversionService = new DefaultConversionService();

			conversionService.addConverter(JpaResultConverters.BlobToByteArrayConverter.INSTANCE);
			conversionService.removeConvertible(Collection.class, Object.class);
			conversionService.removeConvertible(Object.class, Optional.class);

			CONVERSION_SERVICE = conversionService;
		}

		protected final AbstractJpaQueryContext queryContext;

		public JpaQueryContextExecutor(AbstractJpaQueryContext queryContext) {
			this.queryContext = queryContext;
		}

		@Nullable
		public Object execute(AbstractJpaQueryContext queryContext, Query query, JpaParametersParameterAccessor accessor) {

			Assert.notNull(queryContext, "QueryContext must not be null");
			Assert.notNull(query, "Query must not be null");
			Assert.notNull(accessor, "JpaParametersParameterAccessor must not be null");

			Object result;

			try {
				result = doExecute(queryContext, query, accessor);
			} catch (NoResultException ex) {
				return null;
			}

			if (result == null) {
				return null;
			}

			Class<?> requiredType = queryContext.getQueryMethod().getReturnType();

			if (ClassUtils.isAssignable(requiredType, void.class) || ClassUtils.isAssignableValue(requiredType, result)) {
				return result;
			}

			return CONVERSION_SERVICE.canConvert(result.getClass(), requiredType) //
					? CONVERSION_SERVICE.convert(result, requiredType) //
					: result;
		}

		@Nullable
		protected abstract Object doExecute(AbstractJpaQueryContext queryContext, Query query,
				JpaParametersParameterAccessor accessor);
	}

	/**
	 * Execute a JPA query for a Java 8 {@link java.util.stream.Stream}-returning repository method.
	 */
	private class StreamExecutor extends JpaQueryContextExecutor {

		private static final String NO_SURROUNDING_TRANSACTION = "You're trying to execute a streaming query method without a surrounding transaction that keeps the connection open so that the Stream can actually be consumed; Make sure the code consuming the stream uses @Transactional or any other way of declaring a (read-only) transaction";

		public StreamExecutor(AbstractJpaQueryContext queryContext) {
			super(queryContext);
		}

		@Override
		protected Object doExecute(AbstractJpaQueryContext queryContext, Query query,
				JpaParametersParameterAccessor accessor) {

			if (!SurroundingTransactionDetectorMethodInterceptor.INSTANCE.isSurroundingTransactionActive()) {
				throw new InvalidDataAccessApiUsageException(NO_SURROUNDING_TRANSACTION);
			}

			return query.getResultStream();
		}
	}

	/**
	 * Execute a JPA stored procedure.
	 */
	private class ProcedureExecutor extends JpaQueryContextExecutor {

		private static final String NO_SURROUNDING_TRANSACTION = "You're trying to execute a @Procedure method without a surrounding transaction that keeps the connection open so that the ResultSet can actually be consumed; Make sure the consumer code uses @Transactional or any other way of declaring a (read-only) transaction";

		public ProcedureExecutor(AbstractJpaQueryContext context) {

			super(context);

			Assert.isInstanceOf(StoredProcedureQueryContext.class, context);
		}

		protected StoredProcedureQueryContext getProcedureContext() {
			return (StoredProcedureQueryContext) queryContext;
		}

		@Override
		protected Object doExecute(AbstractJpaQueryContext queryContext, Query query,
				JpaParametersParameterAccessor accessor) {

			Assert.isInstanceOf(StoredProcedureQuery.class, query);

			StoredProcedureQuery procedure = (StoredProcedureQuery) query;

			try {
				boolean returnsResultSet = procedure.execute();

				if (returnsResultSet) {

					if (!SurroundingTransactionDetectorMethodInterceptor.INSTANCE.isSurroundingTransactionActive()) {
						throw new InvalidDataAccessApiUsageException(NO_SURROUNDING_TRANSACTION);
					}

					return queryContext.getQueryMethod().isCollectionQuery() ? procedure.getResultList()
							: procedure.getSingleResult();
				}

				return getProcedureContext().extractOutputValue(procedure); // extract output value from the procedure
			} finally {
				if (procedure instanceof AutoCloseable autoCloseable) {
					try {
						autoCloseable.close();
					} catch (Exception ignored) {}
				}
			}
		}
	}

	/**
	 * Execute a JPA query for a Java {@link Collection}-returning repository method.
	 */
	private class CollectionExecutor extends JpaQueryContextExecutor {

		public CollectionExecutor(AbstractJpaQueryContext queryContext) {
			super(queryContext);
		}

		@Override
		protected Object doExecute(AbstractJpaQueryContext queryContext, Query query,
				JpaParametersParameterAccessor accessor) {
			return query.getResultList();
		}
	}

	/**
	 * Execute a JPA query for a Spring Data {@link org.springframework.data.domain.Slice}-returning repository method.
	 */
	private class SlicedExecutor extends JpaQueryContextExecutor {

		public SlicedExecutor(AbstractJpaQueryContext queryContext) {
			super(queryContext);
		}

		@Override
		protected Object doExecute(AbstractJpaQueryContext queryContext, Query query,
				JpaParametersParameterAccessor accessor) {

			Pageable pageable = accessor.getPageable();

			int pageSize = 0;
			if (pageable.isPaged()) {

				pageSize = pageable.getPageSize();
				query.setMaxResults(pageSize + 1);
			}

			List<?> resultList = query.getResultList();

			boolean hasNext = pageable.isPaged() && resultList.size() > pageSize;

			if (hasNext) {
				return new SliceImpl<>(resultList.subList(0, pageSize), pageable, hasNext);
			} else {
				return new SliceImpl<>(resultList, pageable, hasNext);
			}
		}
	}

	/**
	 * Execute a JPA query for a Spring Data {@link org.springframework.data.domain.Page}-returning repository method.
	 */
	private class PagedExecutor extends JpaQueryContextExecutor {

		public PagedExecutor(AbstractJpaQueryContext queryContext) {
			super(queryContext);
		}

		@Override
		protected Object doExecute(AbstractJpaQueryContext queryContext, Query query,
				JpaParametersParameterAccessor accessor) {

			List resultList = query.getResultList();

			return PageableExecutionUtils.getPage(resultList, accessor.getPageable(), () -> count(queryContext, accessor));
		}

		private long count(AbstractJpaQueryContext queryContext, JpaParametersParameterAccessor accessor) {

			List<?> totals = queryContext.createCountQuery(accessor).getResultList();

			return totals.size() == 1 //
					? CONVERSION_SERVICE.convert(totals.get(0), Long.class) //
					: totals.size();
		}

	}

	/**
	 * Execute a JPA query for a repository with an @{@link org.springframework.data.jpa.repository.Modifying} annotation
	 * applied.
	 */
	private class ModifyingExecutor extends JpaQueryContextExecutor {

		private final EntityManager entityManager;

		public ModifyingExecutor(AbstractJpaQueryContext queryContext, EntityManager entityManager) {

			super(queryContext);

			Assert.notNull(entityManager, "EntityManager must not be null");

			this.entityManager = entityManager;
		}

		@Override
		protected Object doExecute(AbstractJpaQueryContext queryContext, Query query,
				JpaParametersParameterAccessor accessor) {

			Class<?> returnType = method.getReturnType();

			boolean isVoid = ClassUtils.isAssignable(returnType, Void.class);
			boolean isInt = ClassUtils.isAssignable(returnType, Integer.class);

			Assert.isTrue(isInt || isVoid,
					"Modifying queries can only use void or int/Integer as return type; Offending method: " + method);

			if (method.getFlushAutomatically()) {
				entityManager.flush();
			}

			int result = query.executeUpdate();

			if (method.getClearAutomatically()) {
				entityManager.clear();
			}

			return result;
		}
	}

	/**
	 * Execute a JPA query for a repository method returning a single value.
	 */
	private class SingleEntityExecutor extends JpaQueryContextExecutor {

		public SingleEntityExecutor(AbstractJpaQueryContext queryContext) {
			super(queryContext);
		}

		@Override
		protected Object doExecute(AbstractJpaQueryContext queryContext, Query query,
				JpaParametersParameterAccessor accessor) {
			return query.getSingleResult();
		}
	}
}
