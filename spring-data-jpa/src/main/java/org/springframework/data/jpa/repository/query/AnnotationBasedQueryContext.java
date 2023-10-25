package org.springframework.data.jpa.repository.query;

import static org.springframework.data.jpa.repository.query.ExpressionBasedStringQuery.*;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import jakarta.persistence.Tuple;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.QueryRewriter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

class AnnotationBasedQueryContext extends AbstractJpaQueryContext {

	private final String originalQueryString;
	private final String queryString;
	private final String countQueryString;
	private final QueryMethodEvaluationContextProvider evaluationContextProvider;
	private final SpelExpressionParser parser;
	private final boolean nativeQuery;
	private final List<ParameterBinding> bindings;
	private final DeclaredQuery declaredQuery;
	private final QueryRewriter queryRewriter;

	public AnnotationBasedQueryContext(JpaQueryMethod method, EntityManager entityManager, String queryString,
			String countQueryString, QueryMethodEvaluationContextProvider evaluationContextProvider,
			SpelExpressionParser parser, boolean nativeQuery, QueryRewriter queryRewriter) {

		super(method, entityManager);

		this.bindings = new ArrayList<>();

		this.originalQueryString = queryString;
		this.evaluationContextProvider = evaluationContextProvider;
		this.parser = parser;
		this.nativeQuery = nativeQuery;

		QueryPair queryPair = renderQuery(queryString, countQueryString);
		this.queryString = queryPair.query();
		this.countQueryString = queryPair.countQuery();

		this.declaredQuery = DeclaredQuery.of(originalQueryString, nativeQuery);
		this.queryRewriter = queryRewriter;

		validateQueries();
	}

	public String getQueryString() {
		return queryString;
	}

	public String getCountQueryString() {
		return countQueryString;
	}

	@Override
	protected String createQuery(JpaParametersParameterAccessor accessor) {
		return queryString;
	}

	@Override
	protected String postProcessQuery(String query, JpaParametersParameterAccessor accessor) {

		DeclaredQuery declaredQuery = DeclaredQuery.of(query, nativeQuery);

		return QueryEnhancerFactory.forQuery(declaredQuery) //
				.applySorting(accessor.getSort(), declaredQuery.getAlias());
	}

	@Override
	protected Query turnIntoJpaQuery(String query, JpaParametersParameterAccessor accessor) {

		ResultProcessor processor = getQueryMethod().getResultProcessor().withDynamicProjection(accessor);

		ReturnedType returnedType = processor.getReturnedType();
		Class<?> typeToRead = getTypeToRead(returnedType);

		String potentiallyRewrittenQuery = potentiallyRewriteQuery(query, accessor);

		if (typeToRead == null) {
			return nativeQuery //
					? getEntityManager().createNativeQuery(potentiallyRewrittenQuery) //
					: getEntityManager().createQuery(potentiallyRewrittenQuery);
		}

		return nativeQuery //
				? getEntityManager().createNativeQuery(potentiallyRewrittenQuery, typeToRead) //
				: getEntityManager().createQuery(potentiallyRewrittenQuery, typeToRead);
	}

	@Override
	protected Class<?> getTypeToRead(ReturnedType returnedType) {

		if (!nativeQuery) {
			return super.getTypeToRead(returnedType);
		}

		Class<?> result = getQueryMethod().isQueryForEntity() ? returnedType.getDomainType() : null;

		if (declaredQuery.hasConstructorExpression() || declaredQuery.isDefaultProjection()) {
			return result;
		}

		return returnedType.isProjecting() && !getMetamodel().isJpaManaged(returnedType.getReturnedType()) //
				? Tuple.class
				: result;
	}

	@Override
	protected Query createCountQuery(JpaParametersParameterAccessor accessor) {

		EntityManager em = getEntityManager();

		Query query = getQueryMethod().isNativeQuery() //
				? em.createNativeQuery(countQueryString) //
				: em.createQuery(countQueryString, Long.class);

		QueryParameterSetter.QueryMetadata metadata = metadataCache.getMetadata(queryString, query);

		parameterBinder.get().bind(metadata.withQuery(query), accessor, QueryParameterSetter.ErrorHandling.LENIENT);

		return query;
	}

	@Override
	protected ParameterBinder createBinder() {
		return ParameterBinderFactory.createQueryAwareBinder(getQueryMethod().getParameters(), declaredQuery, parser,
				evaluationContextProvider);
	}

	@Override
	protected Query bindParameters(Query query, JpaParametersParameterAccessor accessor) {

		QueryParameterSetter.QueryMetadata metadata = metadataCache.getMetadata(queryString, query);

		return parameterBinder.get().bindAndPrepare(query, metadata, accessor);
	}

	// Internals

	private record QueryPair(String query, String countQuery) {
	}

	private QueryPair renderQuery(String initialQuery, String initialCountQuery) {

		if (!containsExpression(initialQuery)) {

			Metadata queryMeta = new Metadata();
			String finalQuery = ParameterBindingParser.INSTANCE
					.parseParameterBindingsOfQueryIntoBindingsAndReturnCleanedQuery(initialQuery, this.bindings, queryMeta);

			return new QueryPair(finalQuery, initialCountQuery);
		}

		StandardEvaluationContext evalContext = new StandardEvaluationContext();
		evalContext.setVariable(ENTITY_NAME, getQueryMethod().getEntityInformation().getEntityName());

		String potentiallyQuotedQueryString = potentiallyQuoteExpressionsParameter(initialQuery);

		Expression expr = parser.parseExpression(potentiallyQuotedQueryString, ParserContext.TEMPLATE_EXPRESSION);

		String result = expr.getValue(evalContext, String.class);

		String processedQuery = result == null //
				? potentiallyQuotedQueryString //
				: potentiallyUnquoteParameterExpressions(result);

		Metadata queryMeta = new Metadata();
		String finalQuery = ParameterBindingParser.INSTANCE
				.parseParameterBindingsOfQueryIntoBindingsAndReturnCleanedQuery(processedQuery, this.bindings, queryMeta);

		return new QueryPair(finalQuery, initialCountQuery);
	}

	private void validateQueries() {

		if (nativeQuery) {

			Parameters<?, ?> parameters = getQueryMethod().getParameters();

			if (parameters.hasSortParameter() && !queryString.contains("#sort")) {
				throw new InvalidJpaQueryMethodException(
						"Cannot use native queries with dynamic sorting in method " + getQueryMethod());
			}
		}

		validateJpaQuery(queryString, "Validation failed for query with method %s", getQueryMethod());

		if (getQueryMethod().isPageQuery() && countQueryString != null) {
			validateJpaQuery(countQueryString,
					String.format("Count query validation failed for method %s", getQueryMethod()));
		}
	}

	private void validateJpaQuery(String query, String errorMessage, Object... arguments) {

		if (getQueryMethod().isProcedureQuery()) {
			return;
		}

		try (EntityManager validatingEm = getEntityManager().getEntityManagerFactory().createEntityManager()) {

			if (nativeQuery) {
				validatingEm.createNativeQuery(query);
			} else {
				validatingEm.createQuery(query);
			}
		} catch (RuntimeException ex) {

			// Needed as there's ambiguities in how an invalid query string shall be expressed by the persistence provider
			// https://java.net/projects/jpa-spec/lists/jsr338-experts/archive/2012-07/message/17
			throw new IllegalArgumentException(String.format(errorMessage, arguments), ex);
		}
	}

	/**
	 * Use the {@link QueryRewriter}, potentially rewrite the query, using relevant {@link Sort} and {@link Pageable}
	 * information.
	 *
	 * @param originalQuery
	 * @param accessor
	 * @return
	 */
	private String potentiallyRewriteQuery(String originalQuery, JpaParametersParameterAccessor accessor) {

		Sort sort = accessor.getSort();
		Pageable pageable = accessor.getPageable();

		return pageable != null && pageable.isPaged() //
				? queryRewriter.rewrite(originalQuery, pageable) //
				: queryRewriter.rewrite(originalQuery, sort);
	}

}
