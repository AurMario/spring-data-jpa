package org.springframework.data.jpa.repository.query;

import static org.springframework.data.jpa.repository.query.ExpressionBasedStringQuery.*;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import jakarta.persistence.Tuple;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.jpa.provider.PersistenceProvider;
import org.springframework.data.jpa.util.JpaMetamodel;
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

	public AnnotationBasedQueryContext(JpaQueryMethod method, EntityManager entityManager, PersistenceProvider provider,
			String queryString, String countQueryString, QueryMethodEvaluationContextProvider evaluationContextProvider,
			SpelExpressionParser parser, boolean nativeQuery) {

		super(method, entityManager, JpaMetamodel.of(entityManager.getMetamodel()), provider);

		this.bindings = new ArrayList<>();

		this.originalQueryString = queryString;
		this.evaluationContextProvider = evaluationContextProvider;
		this.parser = parser;
		this.nativeQuery = nativeQuery;

		QueryPair queryPair = renderQuery(queryString, countQueryString);
		this.queryString = queryPair.query();
		this.countQueryString = queryPair.countQuery();

		this.declaredQuery = DeclaredQuery.of(originalQueryString, nativeQuery);

		validateQueries();
	}

	@Override
	protected ParameterBinder createBinder() {
		return ParameterBinderFactory.createQueryAwareBinder(getQueryMethod().getParameters(), declaredQuery, parser,
				evaluationContextProvider);
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

		// TODO: Figure out how to handle count queries (different part of the top-level flow??
		if (getQueryMethod().isPageQuery() && countQueryString != null) {
			validateJpaQuery(countQueryString,
					String.format("Count query validation failed for method %s", getQueryMethod()));
		}
	}

	public String getQueryString() {
		return queryString;
	}

	public String getCountQueryString() {
		return countQueryString;
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

	private record QueryPair(String query, String countQuery) {
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

	@Override
	protected String createQuery() {
		return queryString;
	}

	@Override
	protected String processQuery(String query, JpaParametersParameterAccessor accessor) {

		DeclaredQuery declaredQuery = DeclaredQuery.of(query, nativeQuery);

		return QueryEnhancerFactory.forQuery(declaredQuery) //
				.applySorting(accessor.getSort(), declaredQuery.getAlias());
	}

	@Override
	protected Query createJpaQuery(String query, JpaParametersParameterAccessor accessor) {

		ResultProcessor processor = getQueryMethod().getResultProcessor().withDynamicProjection(accessor);

		ReturnedType returnedType = processor.getReturnedType();
		Class<?> typeToRead = getTypeToRead(returnedType);

		if (typeToRead == null) {
			return nativeQuery //
					? getEntityManager().createNativeQuery(queryString) //
					: getEntityManager().createQuery(queryString);
		}

		return nativeQuery //
				? getEntityManager().createNativeQuery(queryString, typeToRead) //
				: getEntityManager().createQuery(queryString, typeToRead);
	}

	@Override
	protected Query bind(Query query, JpaParametersParameterAccessor accessor) {

		QueryParameterSetter.QueryMetadata metadata = metadataCache.getMetadata(queryString, query);

		ParameterBinder parameterBinder1 = parameterBinder.get();
		Query boundQuery = parameterBinder1.bindAndPrepare(query, metadata, accessor);

		// TODO: This is for binding the count query.
		// parameterBinder.bind(metadata.withQuery(query), accessor, QueryParameterSetter.ErrorHandling.LENIENT);

		return boundQuery;
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
}
