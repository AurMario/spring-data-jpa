package org.springframework.data.jpa.repository.query;

import jakarta.persistence.EntityManager;
import jakarta.persistence.ParameterMode;
import jakarta.persistence.Query;
import jakarta.persistence.StoredProcedureQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.jpa.provider.PersistenceProvider;
import org.springframework.data.jpa.util.JpaMetamodel;
import org.springframework.data.repository.core.support.SurroundingTransactionDetectorMethodInterceptor;
import org.springframework.data.repository.query.Parameter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

class StoredProcedureQueryContext<T> extends AbstractJpaQueryContext {

	private static final String NO_SURROUNDING_TRANSACTION = "You're trying to execute a @Procedure method without a surrounding transaction that keeps the connection open so that the ResultSet can actually be consumed; Make sure the consumer code uses @Transactional or any other way of declaring a (read-only) transaction";

	private final StoredProcedureAttributes procedureAttributes;
	private final boolean useNamedParameters;
	private final QueryParameterSetter.QueryMetadataCache metadataCache = new QueryParameterSetter.QueryMetadataCache();

	public StoredProcedureQueryContext(JpaQueryMethod method, EntityManager entityManager, JpaMetamodel metamodel,
			PersistenceProvider provider) {

		super(method, entityManager, metamodel, provider);
		this.procedureAttributes = method.getProcedureAttributes();
		this.useNamedParameters = useNamedParameters(method);
	}

	private static boolean useNamedParameters(JpaQueryMethod method) {

		return method.getParameters().stream() //
				.filter(Parameter::isNamedParameter) //
				.findAny() //
				.isPresent();
	}

	@Override
	protected String createQuery() {
		return procedureAttributes.getProcedureName();
	}

	@Override
	protected Query createJpaQuery(String query) {

		return procedureAttributes.isNamedStoredProcedure() //
				? newNamedStoredProcedureQuery(query)
				: newAdhocStoredProcedureQuery(query);
	}

	@Override
	protected Query bind(Query query, JpaParametersParameterAccessor accessor) {

		Assert.isInstanceOf(StoredProcedureQuery.class, query);

		StoredProcedureQuery storedProcedure = (StoredProcedureQuery) query;
		QueryParameterSetter.QueryMetadata metadata = metadataCache.getMetadata("singleton", storedProcedure);

		return parameterBinder.bind(query, metadata, accessor);
	}

	@Override
	protected Object doExecute(JpaQueryMethod method, Query queryToExecute, JpaParametersParameterAccessor accessor) {

		Assert.isInstanceOf(StoredProcedureQuery.class, queryToExecute);

		StoredProcedureQuery procedure = (StoredProcedureQuery) queryToExecute;

		try {
			boolean returnsResultSet = procedure.execute();

			if (returnsResultSet) {

				if (!SurroundingTransactionDetectorMethodInterceptor.INSTANCE.isSurroundingTransactionActive()) {
					throw new InvalidDataAccessApiUsageException(NO_SURROUNDING_TRANSACTION);
				}

				return method.isCollectionQuery() ? procedure.getResultList() : procedure.getSingleResult();
			}

			return extractOutputValue(procedure); // extract output value from the procedure
		} finally {
			if (procedure instanceof AutoCloseable autoCloseable) {
				try {
					autoCloseable.close();
				} catch (Exception ignored) {}
			}
		}
	}

	@Nullable
	Object extractOutputValue(StoredProcedureQuery storedProcedureQuery) {

		Assert.notNull(storedProcedureQuery, "StoredProcedureQuery must not be null");

		if (!procedureAttributes.hasReturnValue()) {
			return null;
		}

		List<ProcedureParameter> outputParameters = procedureAttributes.getOutputProcedureParameters();

		if (outputParameters.size() == 1) {
			return extractOutputParameterValue(outputParameters.get(0), 0, storedProcedureQuery);
		}

		Map<String, Object> outputValues = new HashMap<>();

		for (int i = 0; i < outputParameters.size(); i++) {
			ProcedureParameter outputParameter = outputParameters.get(i);
			outputValues.put(outputParameter.getName(),
					extractOutputParameterValue(outputParameter, i, storedProcedureQuery));
		}

		return outputValues;
	}

	/**
	 * @return The value of an output parameter either by name or by index.
	 */
	private Object extractOutputParameterValue(ProcedureParameter outputParameter, Integer index,
			StoredProcedureQuery storedProcedureQuery) {

		JpaParameters methodParameters = (JpaParameters) getQueryMethod().getParameters();

		return useNamedParameters && StringUtils.hasText(outputParameter.getName())
				? storedProcedureQuery.getOutputParameterValue(outputParameter.getName())
				: storedProcedureQuery.getOutputParameterValue(methodParameters.getNumberOfParameters() + index + 1);
	}

	private Query newNamedStoredProcedureQuery(String query) {
		return getEntityManager().createNamedStoredProcedureQuery(query);
	}

	private Query newAdhocStoredProcedureQuery(String query) {

		StoredProcedureQuery procedureQuery = getQueryMethod().isQueryForEntity() //
				? getEntityManager().createStoredProcedureQuery(query, getQueryMethod().getEntityInformation().getJavaType()) //
				: getEntityManager().createStoredProcedureQuery(query);

		JpaParameters params = (JpaParameters) getQueryMethod().getParameters();

		for (JpaParameters.JpaParameter param : params) {

			if (!param.isBindable()) {
				continue;
			}

			if (useNamedParameters) {
				procedureQuery.registerStoredProcedureParameter(
						param.getName()
								.orElseThrow(() -> new IllegalArgumentException(ParameterBinder.PARAMETER_NEEDS_TO_BE_NAMED)),
						param.getType(), ParameterMode.IN);
			} else {
				procedureQuery.registerStoredProcedureParameter(param.getIndex() + 1, param.getType(), ParameterMode.IN);
			}
		}

		if (procedureAttributes.hasReturnValue()) {

			ProcedureParameter procedureOutput = procedureAttributes.getOutputProcedureParameters().get(0);

			/*
			 * If there is a {@link java.sql.ResultSet} with a {@link ParameterMode#REF_CURSOR}, find the output parameter.
			 * Otherwise, no need, there is no need to find an output parameter.
			 */
			if (storedProcedureHasResultSetUsingRefCursor(procedureOutput) || !isResultSetProcedure()) {

				if (useNamedParameters) {
					procedureQuery.registerStoredProcedureParameter(procedureOutput.getName(), procedureOutput.getType(),
							procedureOutput.getMode());
				} else {

					// Output parameter should be after the input parameters
					int outputParameterIndex = params.getNumberOfParameters() + 1;

					procedureQuery.registerStoredProcedureParameter(outputParameterIndex, procedureOutput.getType(),
							procedureOutput.getMode());
				}
			}
		}

		return procedureQuery;
	}

	/**
	 * Does this stored procedure have a {@link java.sql.ResultSet} using {@link ParameterMode#REF_CURSOR}?
	 *
	 * @param procedureOutput
	 * @return
	 */
	private boolean storedProcedureHasResultSetUsingRefCursor(ProcedureParameter procedureOutput) {
		return isResultSetProcedure() && procedureOutput.getMode() == ParameterMode.REF_CURSOR;
	}

	/**
	 * @return true if the stored procedure will use a ResultSet to return data and not output parameters
	 */
	private boolean isResultSetProcedure() {
		return getQueryMethod().isCollectionQuery() || getQueryMethod().isQueryForEntity();
	}

}
