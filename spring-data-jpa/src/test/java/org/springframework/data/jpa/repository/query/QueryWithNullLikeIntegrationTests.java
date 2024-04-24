/*
 * Copyright 2012-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jpa.repository.query;

import static org.assertj.core.api.Assertions.*;

import jakarta.persistence.EntityManagerFactory;

import java.util.List;
import java.util.Properties;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.FilterType;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.data.jpa.domain.sample.EmployeeWithName;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.data.repository.query.Param;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.lang.Nullable;
import org.springframework.orm.jpa.AbstractEntityManagerFactoryBean;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

/**
 * Verify that {@literal LIKE}s mixed with {@literal NULL}s work properly.
 *
 * @author Greg Turnquist
 * @author Yuriy Tsarkov
 * @author Julia Lee
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = QueryWithNullLikeIntegrationTests.Config.class)
@Transactional
class QueryWithNullLikeIntegrationTests {

	@Autowired EmployeeWithNullLikeRepository repository;

	@BeforeEach
	void setUp() {
		repository.saveAllAndFlush(List.of( //
				new EmployeeWithName("Frodo Baggins"), //
				new EmployeeWithName("Bilbo Baggins"),
				new EmployeeWithName(null)));
	}

	@Test
	void customQueryWithMultipleMatch() {

		List<EmployeeWithName> employees = repository.customQueryWithNullableParam("Baggins");

		assertThat(employees).extracting(EmployeeWithName::getName).containsExactlyInAnyOrder("Frodo Baggins",
				"Bilbo Baggins");
	}

	@Test
	void customQueryWithSingleMatch() {

		List<EmployeeWithName> employees = repository.customQueryWithNullableParam("Frodo");

		assertThat(employees).extracting(EmployeeWithName::getName).containsExactlyInAnyOrder("Frodo Baggins");
	}

	@Test
	void customQueryWithEmptyStringMatch() {

		List<EmployeeWithName> employees = repository.customQueryWithNullableParam("");

		assertThat(employees).extracting(EmployeeWithName::getName).containsExactlyInAnyOrder("Frodo Baggins",
				"Bilbo Baggins");
	}

	@Test
	void customQueryWithNullMatch() {

		List<EmployeeWithName> employees = repository.customQueryWithNullableParam(null);

		assertThat(employees).extracting(EmployeeWithName::getName).isEmpty();
	}

	@Test // GH-2939
	void customQueryWithMultipleMatchInNative() {

		List<EmployeeWithName> employees = repository.customQueryWithNullableParamInNative("Baggins");

		assertThat(employees).extracting(EmployeeWithName::getName).containsExactlyInAnyOrder("Frodo Baggins",
				"Bilbo Baggins");
	}

	@Test // GH-2939
	void customQueryWithSingleMatchInNative() {

		List<EmployeeWithName> employees = repository.customQueryWithNullableParamInNative("Frodo");

		assertThat(employees).extracting(EmployeeWithName::getName).containsExactlyInAnyOrder("Frodo Baggins");
	}

	@Test
	void customQueryWithEmptyStringMatchInNative() {

		List<EmployeeWithName> employees = repository.customQueryWithNullableParamInNative("");

		assertThat(employees).extracting(EmployeeWithName::getName).containsExactlyInAnyOrder("Frodo Baggins",
				"Bilbo Baggins");
	}

	@Test // GH-2939
	void customQueryWithNullMatchInNative() {

		List<EmployeeWithName> employees = repository.customQueryWithNullableParamInNative(null);

		assertThat(employees).extracting(EmployeeWithName::getName).isEmpty();
	}

	@Test
	void derivedQueryStartsWithSingleMatch() {

		List<EmployeeWithName> employees = repository.findByNameStartsWith("Frodo");

		assertThat(employees).extracting(EmployeeWithName::getName).containsExactlyInAnyOrder("Frodo Baggins");
	}

	@Test
	void derivedQueryStartsWithNoMatch() {

		List<EmployeeWithName> employees = repository.findByNameStartsWith("Baggins");

		assertThat(employees).extracting(EmployeeWithName::getName).isEmpty();
	}

	@Test
	void derivedQueryStartsWithWithEmptyStringMatch() {

		List<EmployeeWithName> employees = repository.findByNameStartsWith("");

		assertThat(employees).extracting(EmployeeWithName::getName).containsExactlyInAnyOrder("Frodo Baggins",
				"Bilbo Baggins");
	}

	@Test
	void derivedQueryStartsWithWithNullMatch() {

		List<EmployeeWithName> employees = repository.findByNameStartsWith(null);

		assertThat(employees).extracting(EmployeeWithName::getName).isEmpty();
	}

	@Test
	void derivedQueryEndsWithWithMultipleMatch() {

		List<EmployeeWithName> employees = repository.findByNameEndsWith("Baggins");

		assertThat(employees).extracting(EmployeeWithName::getName).containsExactlyInAnyOrder("Frodo Baggins",
				"Bilbo Baggins");
	}

	@Test
	void derivedQueryEndsWithWithSingleMatch() {

		List<EmployeeWithName> employees = repository.findByNameEndsWith("Frodo");

		assertThat(employees).extracting(EmployeeWithName::getName).isEmpty();
	}

	@Test
	void derivedQueryEndsWithWithEmptyStringMatch() {

		List<EmployeeWithName> employees = repository.findByNameEndsWith("");

		assertThat(employees).extracting(EmployeeWithName::getName).containsExactlyInAnyOrder("Frodo Baggins",
				"Bilbo Baggins");
	}

	@Test
	void derivedQueryEndsWithWithNullMatch() {

		List<EmployeeWithName> employees = repository.findByNameEndsWith(null);

		assertThat(employees).extracting(EmployeeWithName::getName).isEmpty();
	}

	@Test
	void derivedQueryContainsWithMultipleMatch() {

		List<EmployeeWithName> employees = repository.findByNameContains("Baggins");

		assertThat(employees).extracting(EmployeeWithName::getName).containsExactlyInAnyOrder("Frodo Baggins",
				"Bilbo Baggins");
	}

	@Test
	void derivedQueryContainsWithSingleMatch() {

		List<EmployeeWithName> employees = repository.findByNameContains("Frodo");

		assertThat(employees).extracting(EmployeeWithName::getName).containsExactly("Frodo Baggins");
	}

	@Test
	void derivedQueryContainsWithEmptyStringMatch() {

		List<EmployeeWithName> employees = repository.findByNameContains("");

		assertThat(employees).extracting(EmployeeWithName::getName).containsExactlyInAnyOrder("Frodo Baggins",
				"Bilbo Baggins");
	}

	@Test
	void derivedQueryContainsWithNullMatch() {

		List<EmployeeWithName> employees = repository.findByNameContains(null);

		assertThat(employees).extracting(EmployeeWithName::getName).isEmpty();
	}

	@Test
	void derivedQueryLikeWithMultipleMatch() {

		List<EmployeeWithName> employees = repository.findByNameLike("%Baggins%");

		assertThat(employees).extracting(EmployeeWithName::getName).containsExactlyInAnyOrder("Frodo Baggins",
				"Bilbo Baggins");
	}

	@Test
	void derivedQueryLikeWithSingleMatch() {

		List<EmployeeWithName> employees = repository.findByNameLike("%Frodo%");

		assertThat(employees).extracting(EmployeeWithName::getName).containsExactly("Frodo Baggins");
	}

	@Test
	void derivedQueryLikeWithEmptyStringMatch() {

		List<EmployeeWithName> employees = repository.findByNameLike("%%");

		assertThat(employees).extracting(EmployeeWithName::getName).containsExactlyInAnyOrder("Frodo Baggins",
				"Bilbo Baggins");
	}

	@Test // GH-1184
	void mismatchedReturnTypeShouldCauseException() {
		assertThatExceptionOfType(ConversionFailedException.class)
				.isThrownBy(() -> repository.customQueryWithMismatchedReturnType());
	}

	@Test // GH-1184
	void alignedReturnTypeShouldWork() {
		assertThat(repository.customQueryWithAlignedReturnType()).containsExactly(new Object[][] {
				{ "Frodo Baggins", "Frodo Baggins with suffix" }, { "Bilbo Baggins", "Bilbo Baggins with suffix" }, { null, null} });
	}

	@Test // GH-3137
	void nullOptionalParameterShouldReturnAllEntries() {

		List<EmployeeWithName> result = repository.customQueryWithOptionalParameter(null);

		assertThat(result).hasSize(3);
	}

	@Transactional
	public interface EmployeeWithNullLikeRepository extends JpaRepository<EmployeeWithName, Integer> {

		@Query("select e from EmployeeWithName e where e.name like %:partialName%")
		List<EmployeeWithName> customQueryWithNullableParam(@Nullable @Param("partialName") String partialName);

		@Query("select e.name, concat(e.name, ' with suffix') from EmployeeWithName e")
		List<EmployeeWithName> customQueryWithMismatchedReturnType();

		@Query("select e.name, concat(e.name, ' with suffix') from EmployeeWithName e")
		List<Object[]> customQueryWithAlignedReturnType();

		@Query(value = "select * from EmployeeWithName as e where e.name like %:partialName%", nativeQuery = true)
		List<EmployeeWithName> customQueryWithNullableParamInNative(@Nullable @Param("partialName") String partialName);

		@Query("select e from EmployeeWithName e where (:partialName is null or e.name like %:partialName%)")
		List<EmployeeWithName> customQueryWithOptionalParameter(@Nullable @Param("partialName") String partialName);

		List<EmployeeWithName> findByNameStartsWith(@Nullable String partialName);

		List<EmployeeWithName> findByNameEndsWith(@Nullable String partialName);

		List<EmployeeWithName> findByNameContains(@Nullable String partialName);

		List<EmployeeWithName> findByNameLike(@Nullable String partialName);
	}

	@EnableJpaRepositories(considerNestedRepositories = true, //
			includeFilters = @Filter(type = FilterType.ASSIGNABLE_TYPE, classes = EmployeeWithNullLikeRepository.class))
	@EnableTransactionManagement
	static class Config {

		@Bean
		DataSource dataSource() {
			return new EmbeddedDatabaseBuilder().generateUniqueName(true).build();
		}

		@Bean
		AbstractEntityManagerFactoryBean entityManagerFactory(DataSource dataSource) {

			LocalContainerEntityManagerFactoryBean factoryBean = new LocalContainerEntityManagerFactoryBean();
			factoryBean.setDataSource(dataSource);
			factoryBean.setPersistenceUnitName("spring-data-jpa");
			factoryBean.setJpaVendorAdapter(new HibernateJpaVendorAdapter());

			Properties properties = new Properties();
			properties.setProperty("hibernate.hbm2ddl.auto", "create");
			factoryBean.setJpaProperties(properties);

			return factoryBean;
		}

		@Bean
		PlatformTransactionManager transactionManager(EntityManagerFactory emf) {
			return new JpaTransactionManager(emf);
		}
	}
}
