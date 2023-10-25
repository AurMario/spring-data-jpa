package org.springframework.data.jpa.repository.query;

import static org.springframework.data.repository.query.parser.Part.Type.*;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceUnitUtil;
import jakarta.persistence.Query;

import java.util.Iterator;

import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.support.JpaMetamodelEntityInformation;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.util.Streamable;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

class CustomFinderQueryContext extends AbstractJpaQueryContext {

	private final JpaParameters parameters;
	private final PartTree tree;
	private final JpaMetamodelEntityInformation<?, Object> entityInformation;
	private final boolean recreationRequired;

	public CustomFinderQueryContext(JpaQueryMethod method, EntityManager entityManager) {

		super(method, entityManager, null, null);

		this.parameters = method.getParameters();

		Class<?> domainClass = method.getEntityInformation().getJavaType();
		PersistenceUnitUtil persistenceUnitUtil = entityManager.getEntityManagerFactory().getPersistenceUnitUtil();
		this.entityInformation = new JpaMetamodelEntityInformation<>(domainClass, entityManager.getMetamodel(),
				persistenceUnitUtil);

		this.recreationRequired = parameters.hasDynamicProjection() || parameters.potentiallySortsDynamically()
				|| method.isScrollQuery();

		try {

			this.tree = new PartTree(method.getName(), domainClass);

			validate(tree, parameters, method.getName());

		} catch (Exception ex) {
			throw new IllegalArgumentException(
					String.format("Failed to create query for method %s; %s", method, ex.getMessage()), ex);
		}
	}

	@Override
	protected String createQuery(JpaParametersParameterAccessor accessor) {

		QueryCreator queryCreator = new QueryCreator(recreationRequired, accessor);

		Sort dynamicSort = getDynamicSort(accessor);
		String query = queryCreator.createQuery(dynamicSort);

		System.out.println(query);

		return query;
	}

	Sort getDynamicSort(JpaParametersParameterAccessor accessor) {

		return accessor.getParameters().potentiallySortsDynamically() //
				? accessor.getSort() //
				: Sort.unsorted();
	}

	@Override
	protected Query createCountQuery(JpaParametersParameterAccessor accessor) {

		CountQueryCreator queryCreator = new CountQueryCreator(recreationRequired, accessor);

		String countQuery = queryCreator.createQuery();

		return getEntityManager().createQuery(countQuery, Long.class);
	}

	@Override
	protected Query turnIntoJpaQuery(String query, JpaParametersParameterAccessor accessor) {
		return super.turnIntoJpaQuery(query, accessor);
	}

	@Override
	protected Query bindParameters(Query query, JpaParametersParameterAccessor accessor) {

		QueryParameterSetter.QueryMetadata metadata = metadataCache.getMetadata("query", query);
		ParameterBinder binder = ParameterBinderFactory.createBinder((JpaParameters) accessor.getParameters());

		if (binder == null) {
			throw new IllegalStateException("ParameterBinder is null");
		}

		Query boundQuery = binder.bindAndPrepare(query, metadata, accessor);
		return boundQuery;
	}

	private void validate(PartTree tree, JpaParameters parameters, String methodName) {

		int argCount = 0;

		Iterable<Part> parts = () -> tree.stream().flatMap(Streamable::stream).iterator();

		for (Part part : parts) {

			int numberOfArguments = part.getNumberOfArguments();

			for (int i = 0; i < numberOfArguments; i++) {

				throwExceptionOnArgumentMismatch(methodName, part, parameters, argCount);

				argCount++;
			}
		}
	}

	private static void throwExceptionOnArgumentMismatch(String methodName, Part part, JpaParameters parameters,
			int index) {

		Part.Type type = part.getType();
		String property = part.getProperty().toDotPath();

		if (!parameters.getBindableParameters().hasParameterAt(index)) {
			throw new IllegalStateException(String.format(
					"Method %s expects at least %d arguments but only found %d; This leaves an operator of type %s for property %s unbound",
					methodName, index + 1, index, type.name(), property));
		}

		JpaParameters.JpaParameter parameter = parameters.getBindableParameter(index);

		if (expectsCollection(type) && !parameterIsCollectionLike(parameter)) {
			throw new IllegalStateException(wrongParameterTypeMessage(methodName, property, type, "Collection", parameter));
		} else if (!expectsCollection(type) && !parameterIsScalarLike(parameter)) {
			throw new IllegalStateException(wrongParameterTypeMessage(methodName, property, type, "scalar", parameter));
		}
	}

	private static boolean expectsCollection(Part.Type type) {
		return type == Part.Type.IN || type == Part.Type.NOT_IN;
	}

	private static boolean parameterIsCollectionLike(JpaParameters.JpaParameter parameter) {
		return Iterable.class.isAssignableFrom(parameter.getType()) || parameter.getType().isArray();
	}

	private static String wrongParameterTypeMessage(String methodName, String property, Part.Type operatorType,
			String expectedArgumentType, JpaParameters.JpaParameter parameter) {

		return String.format("Operator %s on %s requires a %s argument, found %s in method %s", operatorType.name(),
				property, expectedArgumentType, parameter.getType(), methodName);
	}

	/**
	 * Arrays are may be treated as collection like or in the case of binary data as scalar
	 */
	private static boolean parameterIsScalarLike(JpaParameters.JpaParameter parameter) {
		return !Iterable.class.isAssignableFrom(parameter.getType());
	}

	private class QueryCreator extends AbstractQueryCreator<String, String> {

		private final boolean recreationRequired;
		private Iterable<? extends Parameter> parameterProvider;
		protected final ReturnedType returnedType;

		public QueryCreator(boolean recreationRequired, JpaParametersParameterAccessor accessor) {

			super(tree, accessor);

			this.recreationRequired = recreationRequired;
			this.parameterProvider = accessor.getParameters();
			this.returnedType = getQueryMethod().getResultProcessor().withDynamicProjection(accessor).getReturnedType();
		}

		@Override
		protected String create(Part part, Iterator<Object> iterator) {
			return toPredicate(part);
		}

		@Override
		protected String and(Part part, String base, Iterator<Object> iterator) {
			return base + " and " + toPredicate(part);
		}

		@Override
		protected String or(String base, String predicate) {
			return base + " or " + predicate;
		}

		@Override
		protected String complete(String criteria, @Nullable Sort sort) {

			String simpleName = returnedType.getDomainType().getSimpleName();
			String simpleAlias = simpleName.substring(0, 1).toLowerCase();

			String query = String.format("select %s from %s %s", simpleAlias, simpleName, simpleAlias);

			if (criteria != null) {
				query += " where " + criteria;
			}

			if (sort != null && sort.isSorted()) {
				query += " order by " + String.join(",", QueryUtils.toOrders(sort, returnedType.getDomainType()));
			}

			return query;
		}

		private String toPredicate(Part part) {
			return new PredicateBuilder(part, parameterProvider).build();
		}
	}

	private class CountQueryCreator extends QueryCreator {

		public CountQueryCreator(boolean recreationRequired, JpaParametersParameterAccessor accessor) {
			super(recreationRequired, accessor);
		}

		@Override
		protected String complete(String criteria, Sort sort) {

			String simpleName = returnedType.getReturnedType().getSimpleName();
			String simpleAlias = simpleName.substring(0, 1).toLowerCase();
			String query = String.format("select count(%s) from %s %s ", simpleAlias, simpleName, simpleAlias);

			if (criteria != null) {
				query += "where " + criteria;
			}

			return query;
		}
	}

	private class PredicateBuilder {
		private final Part part;
		private final Iterator<? extends Parameter> parameters;

		public PredicateBuilder(Part part, Iterable<? extends Parameter> parameters) {

			Assert.notNull(part, "Part must not be null");
			Assert.notNull(parameters, "Parameters must not be null");

			this.part = part;
			this.parameters = parameters.iterator();
		}

		public String build() {

			PropertyPath property = part.getProperty();
			Part.Type type = part.getType();

			String simpleName = property.getOwningType().getType().getSimpleName();
			String simpleAlias = simpleName.substring(0, 1).toLowerCase();

			switch (type) {
				case BETWEEN:
					Parameter first = parameters.next();
					Parameter second = parameters.next();
					return getComparablePath(part) + " between " + first.toString() + " and " + second.toString();
				case AFTER:
				case GREATER_THAN:
					return getComparablePath(part) + " > " + nextParameter();
				case GREATER_THAN_EQUAL:
					return getComparablePath(part) + " >= " + nextParameter();
				case BEFORE:
				case LESS_THAN:
					return getComparablePath(part) + " < " + nextParameter();
				case LESS_THAN_EQUAL:
					return getComparablePath(part) + " <= " + nextParameter();
				case IS_NULL:
					return getTypedPath(simpleAlias, part) + " IS NULL";
				case IS_NOT_NULL:
					return getTypedPath(simpleAlias, part) + " IS NOT NULL";
				case NOT_IN:
					return upperIfIgnoreCase(getTypedPath(simpleAlias, part)) + " NOT IN " + nextParameter();
				case IN:
					String typedPath = getTypedPath(simpleAlias, part);
					String s = upperIfIgnoreCase(typedPath) + " IN " + nextParameter();
					return s;
				case STARTING_WITH:
				case ENDING_WITH:
				case CONTAINING:
				case NOT_CONTAINING:

					if (property.getLeafProperty().isCollection()) {

						String propertyExpression = traversePath(simpleAlias, property);
						String parameterExpression = nextParameter();

						// Can't just call .not() in case of negation as EclipseLink chokes on that.
						return type.equals(NOT_CONTAINING) //
								? isNotMember(parameterExpression, propertyExpression) //
								: isMember(parameterExpression, propertyExpression);
					}

				case LIKE:
				case NOT_LIKE:
					String propertyExpression = upperIfIgnoreCase(getTypedPath(part));
					String parameterExpression = upperIfIgnoreCase(nextParameter());
					return type.equals(LIKE) //
							? propertyExpression + " LIKE " + parameterExpression //
							: propertyExpression + " NOT LIKE " + parameterExpression;
				case TRUE:
					return getTypedPath(simpleAlias, part) + " IS TRUE";
				case FALSE:
					return getTypedPath(simpleAlias, part) + " IS FALSE";
				case SIMPLE_PROPERTY:
					return upperIfIgnoreCase(getTypedPath(simpleAlias, part)) + " = " + upperIfIgnoreCase(nextParameter());
				case NEGATING_SIMPLE_PROPERTY:
					return upperIfIgnoreCase(getTypedPath(simpleAlias, part)) + " <> " + upperIfIgnoreCase(nextParameter());
				case IS_EMPTY:
				case IS_NOT_EMPTY:

					if (!property.getLeafProperty().isCollection()) {
						throw new IllegalArgumentException("IsEmpty / IsNotEmpty can only be used on collection properties");
					}

					String collectionPath = traversePath("", property);

					return type.equals(IS_NOT_EMPTY) //
							? isNotEmpty(collectionPath) //
							: isEmpty(collectionPath);
				default:
					throw new IllegalArgumentException("Unsupported keyword " + type);
			}
		}

		private String upperIfIgnoreCase(String typedPath) {

			switch (part.shouldIgnoreCase()) {

				case ALWAYS:

					Assert.state(canUpperCase(part.getProperty()),
							"Unable to ignore case of " + part.getProperty().getType().getName() + " types, the property '"
									+ part.getProperty().getSegment() + "' must reference a String");
					return "upper(" + typedPath + ")";
				case WHEN_POSSIBLE:

					if (canUpperCase(part.getProperty())) {
						return "upper(" + typedPath + ")";
					}
				case NEVER:
				default:
					return typedPath;
			}
		}

		private boolean canUpperCase(PropertyPath property) {
			return String.class.equals(property.getType());
		}

		private String isNotMember(String parameterExpression, String propertyExpression) {
			return parameterExpression + " NOT MEMBER OF " + propertyExpression;
		}

		private String isMember(String parameterExpression, String propertyExpression) {
			return parameterExpression + " MEMBER OF " + propertyExpression;
		}

		private String isNotEmpty(String collectionPath) {
			return collectionPath + " is not empty";
		}

		private String isEmpty(String collectionPath) {
			return collectionPath + " is empty";
		}

		private String nextParameter() {
			return ":" + parameters.next().getName().get();
		}

		private String getComparablePath(Part part) {
			return getTypedPath(part);
		}

		private String getTypedPath(Part part) {
			return getTypedPath("", part);
		}

		private String getTypedPath(@Nullable String prefix, Part part) {

			return prefix == null || prefix.isEmpty() //
					? part.getProperty().getSegment() //
					: prefix + "." + part.getProperty().getSegment();
		}

		private String traversePath(String totalPath, PropertyPath path) {

			String result = totalPath.isEmpty() ? path.getSegment() : totalPath + "." + path.getSegment();

			return path.hasNext() //
					? traversePath(result, path.next()) //
					: result;
		}
	}
}
