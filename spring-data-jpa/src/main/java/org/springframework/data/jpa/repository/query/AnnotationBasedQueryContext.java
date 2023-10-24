package org.springframework.data.jpa.repository.query;

import static org.springframework.data.jpa.repository.query.ExpressionBasedStringQuery.*;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.jpa.provider.PersistenceProvider;
import org.springframework.data.jpa.util.JpaMetamodel;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

class AnnotationBasedQueryContext extends AbstractJpaQueryContext {

	private final String queryString;
	private final String countQueryString;
	private final QueryMethodEvaluationContextProvider evaluationContextProvider;
	private final SpelExpressionParser parser;
	private final boolean nativeQuery;

	private final List<ParameterBinding> bindings;

	public AnnotationBasedQueryContext(JpaQueryMethod method, EntityManager entityManager, JpaMetamodel metamodel,
			PersistenceProvider provider, String queryString, String countQueryString,
			QueryMethodEvaluationContextProvider evaluationContextProvider, SpelExpressionParser parser,
			boolean nativeQuery) {

		super(method, entityManager, metamodel, provider);

		this.bindings = new ArrayList<>();

		this.evaluationContextProvider = evaluationContextProvider;
		this.parser = parser;
		this.nativeQuery = nativeQuery;

		QueryPair queryPair = renderQuery(queryString, countQueryString);
		this.queryString = queryPair.query();
		this.countQueryString = queryPair.countQuery();

		validateQueries();
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
		// if (getQueryMethod().isPageQuery()) {
		// validateJpaQuery(countQueryString,
		// String.format("Count query validation failed for method %s", getQueryMethod()));
		// }
	}

	public String getQueryString() {
		return queryString;
	}

	public String getCountQueryString() {
		return countQueryString;
	}

	@Override
	protected String createQuery() {
		return queryString;
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
	protected String processQuery(String query, JpaParametersParameterAccessor accessor) {

		DeclaredQuery declaredQuery = DeclaredQuery.of(query, nativeQuery);

		return QueryEnhancerFactory.forQuery(declaredQuery) //
				.applySorting(accessor.getSort(), declaredQuery.getAlias());
	}

	@Override
	protected Query createJpaQuery(String query, JpaParametersParameterAccessor accessor) {

		ResultProcessor processor = getQueryMethod().getResultProcessor().withDynamicProjection(accessor);

		Class<?> typeToRead = getTypeToRead(processor.getReturnedType());

		return nativeQuery //
				? getEntityManager().createNativeQuery(queryString, typeToRead) //
				: getEntityManager().createQuery(queryString, typeToRead);
	}

	@Override
	protected Query bind(Query query, JpaParametersParameterAccessor accessor) {

		QueryParameterSetter.QueryMetadata metadata = metadataCache.getMetadata(queryString, query);

		parameterBinder.bind(metadata.withQuery(query), accessor, QueryParameterSetter.ErrorHandling.LENIENT);

		return query;
	}
}
