package org.springframework.data.jpa.repository.query;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;

class CustomFinderQueryContext extends AbstractJpaQueryContext {

	public CustomFinderQueryContext(JpaQueryMethod method, EntityManager entityManager) {
		super(method, entityManager, null, null);
	}

	@Override
	protected String createQuery() {
		return null;
	}

	@Override
	protected Query bindParameters(Query query, JpaParametersParameterAccessor accessor) {
		return null;
	}
}
