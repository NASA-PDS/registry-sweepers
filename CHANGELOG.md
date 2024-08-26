# Changelog

## [«unknown»](https://github.com/NASA-PDS/registry-sweepers/tree/«unknown») (2024-08-26)

[Full Changelog](https://github.com/NASA-PDS/registry-sweepers/compare/v1.2.1...«unknown»)

**Requirements:**

- Investigate/implement non-redundant ancestry processing [\#91](https://github.com/NASA-PDS/registry-sweepers/issues/91)

**Improvements:**

- Investigate/implement non-redundant provenance processing [\#92](https://github.com/NASA-PDS/registry-sweepers/issues/92)

**Defects:**

- Nonaggregate products present in "foreign" collections/bundles do not have correct ancestry. [\#114](https://github.com/NASA-PDS/registry-sweepers/issues/114)
- `superseded_by` fields with `null` values causing multiple versions of same products to appear in API searches [\#112](https://github.com/NASA-PDS/registry-sweepers/issues/112) [[s.high](https://github.com/NASA-PDS/registry-sweepers/labels/s.high)]
- Timeout bug when running legacy dashboard sync [\#111](https://github.com/NASA-PDS/registry-sweepers/issues/111) [[s.high](https://github.com/NASA-PDS/registry-sweepers/labels/s.high)]
- When building registry-sweeper version 1.2.1, docker image 1.3.0 was published on docker hub [\#109](https://github.com/NASA-PDS/registry-sweepers/issues/109) [[s.medium](https://github.com/NASA-PDS/registry-sweepers/labels/s.medium)]

**Other closed issues:**

- Registry-sweeper upgrade for multitenant registry [\#120](https://github.com/NASA-PDS/registry-sweepers/issues/120) [[s.high](https://github.com/NASA-PDS/registry-sweepers/labels/s.high)]
- Run sweepers locally against PSA prod [\#108](https://github.com/NASA-PDS/registry-sweepers/issues/108) [[s.high](https://github.com/NASA-PDS/registry-sweepers/labels/s.high)]
- Test latest sweeper changes on all production OpenSearch clusters [\#98](https://github.com/NASA-PDS/registry-sweepers/issues/98)
- Add node information to select log messages [\#18](https://github.com/NASA-PDS/registry-sweepers/issues/18)
- Implement functional tests for provenance.py [\#13](https://github.com/NASA-PDS/registry-sweepers/issues/13)
- Switch utils from raw requests calls to opensearch-py [\#12](https://github.com/NASA-PDS/registry-sweepers/issues/12)

## [v1.2.1](https://github.com/NASA-PDS/registry-sweepers/tree/v1.2.1) (2024-01-24)

[Full Changelog](https://github.com/NASA-PDS/registry-sweepers/compare/v1.2.0...v1.2.1)

**Defects:**

- Property values returned by the API are inconsistent, as list or single value. [\#86](https://github.com/NASA-PDS/registry-sweepers/issues/86) [[s.high](https://github.com/NASA-PDS/registry-sweepers/labels/s.high)]

**Other closed issues:**

- docs build fails due to dependency incompatibility [\#94](https://github.com/NASA-PDS/registry-sweepers/issues/94)
- Refresh ancestry metadata on all nodes [\#85](https://github.com/NASA-PDS/registry-sweepers/issues/85)
- ensure ancestry metadata key presence in index [\#83](https://github.com/NASA-PDS/registry-sweepers/issues/83)
- Profile memory usage [\#39](https://github.com/NASA-PDS/registry-sweepers/issues/39) [[s.high](https://github.com/NASA-PDS/registry-sweepers/labels/s.high)]
- registry-sweeper deployment on AWS \(prod\) [\#17](https://github.com/NASA-PDS/registry-sweepers/issues/17)

## [v1.2.0](https://github.com/NASA-PDS/registry-sweepers/tree/v1.2.0) (2023-10-09)

[Full Changelog](https://github.com/NASA-PDS/registry-sweepers/compare/v1.0.0...v1.2.0)

**Improvements:**

- Update repairkit to include repairkit version metadata and check to streamline execution [\#70](https://github.com/NASA-PDS/registry-sweepers/issues/70) [[s.high](https://github.com/NASA-PDS/registry-sweepers/labels/s.high)]

**Defects:**

- Fatal error upon unexpected error due to bad json path [\#37](https://github.com/NASA-PDS/registry-sweepers/issues/37)
- Provenance bulk update db writes fail under specific conditions related to presence of CCRs [\#34](https://github.com/NASA-PDS/registry-sweepers/issues/34) [[s.critical](https://github.com/NASA-PDS/registry-sweepers/labels/s.critical)]
- Properly resolve errors during db write operations [\#32](https://github.com/NASA-PDS/registry-sweepers/issues/32)
- registry-sweepers breaks when run against production-scale data [\#28](https://github.com/NASA-PDS/registry-sweepers/issues/28)
- Malformed product docs break ancestry sweeper [\#20](https://github.com/NASA-PDS/registry-sweepers/issues/20)
- Bundle documents with string-like reference properties are not handled well [\#19](https://github.com/NASA-PDS/registry-sweepers/issues/19)

**Other closed issues:**

- Standardize CICD on python 3.9 [\#78](https://github.com/NASA-PDS/registry-sweepers/issues/78)
- Registry-Sweepers Error: contained no hits when hits were expected [\#69](https://github.com/NASA-PDS/registry-sweepers/issues/69) [[s.high](https://github.com/NASA-PDS/registry-sweepers/labels/s.high)]
- Fix bugged retry functionality [\#66](https://github.com/NASA-PDS/registry-sweepers/issues/66)
- Empty write HTTP400 regression [\#64](https://github.com/NASA-PDS/registry-sweepers/issues/64)
- Deploy repairkit sweeper to delta and prod [\#61](https://github.com/NASA-PDS/registry-sweepers/issues/61)
- Registry-Sweeper ECS Enchancements \(Pre Multi-Tenancy\) [\#59](https://github.com/NASA-PDS/registry-sweepers/issues/59)
- Regularly synchronize the legacy registry collections on Solr in the registry in opensearch [\#58](https://github.com/NASA-PDS/registry-sweepers/issues/58)
- Remedy absent retry-behaviour [\#56](https://github.com/NASA-PDS/registry-sweepers/issues/56)
- Remediate findings from Provenance Script Testing [\#55](https://github.com/NASA-PDS/registry-sweepers/issues/55)
- Create registry-sweeper cluster and deploy/schedule the tasks [\#52](https://github.com/NASA-PDS/registry-sweepers/issues/52)
- Tweak \_bulk flush threshold [\#46](https://github.com/NASA-PDS/registry-sweepers/issues/46)
- Remove CCS support [\#43](https://github.com/NASA-PDS/registry-sweepers/issues/43)
- Consider suppress-data-warnings option [\#40](https://github.com/NASA-PDS/registry-sweepers/issues/40) [[s.high](https://github.com/NASA-PDS/registry-sweepers/labels/s.high)]
- Review CCR behaviour [\#36](https://github.com/NASA-PDS/registry-sweepers/issues/36)
- Configure monitoring for Registry Sweeper ECS cluster [\#30](https://github.com/NASA-PDS/registry-sweepers/issues/30)
- Catch empty remotes list and add logging to sweepers\_driver [\#21](https://github.com/NASA-PDS/registry-sweepers/issues/21)
- Prepare the deployment of the registry-sweeper on AWS \(stage\) [\#16](https://github.com/NASA-PDS/registry-sweepers/issues/16)
- Implement ancestry [\#14](https://github.com/NASA-PDS/registry-sweepers/issues/14)
- Add a configuration for the cloudwatch event monitoring  [\#9](https://github.com/NASA-PDS/registry-sweepers/issues/9)

## [v1.0.0](https://github.com/NASA-PDS/registry-sweepers/tree/v1.0.0) (2023-04-27)

[Full Changelog](https://github.com/NASA-PDS/registry-sweepers/compare/3fabe85a65d26f2509c830d02c2b5f09a2793cf7...v1.0.0)

**Other closed issues:**

- Create the docker image and push it to docker hub as part of the CICD [\#6](https://github.com/NASA-PDS/registry-sweepers/issues/6)
- Update non-essential Dockerfile elements for new repository structure  [\#2](https://github.com/NASA-PDS/registry-sweepers/issues/2)
- Migrate registry-api/provenance script into this repository [\#1](https://github.com/NASA-PDS/registry-sweepers/issues/1)



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
