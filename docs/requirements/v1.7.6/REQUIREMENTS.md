
Requirements Summary
====================

# default

## As a user, I want to know when products queried through the API are off-line ([#152](https://github.com/NASA-PDS/registry-sweepers/issues/152)) 


This requirement is not impacted by the current version
## As a user, I want to know if the datasets available in the registry are published and visible in the API ([#75](https://github.com/NASA-PDS/registry-sweepers/issues/75)) 


This requirement is not impacted by the current version
## Investigate/implement non-redundant ancestry processing ([#91](https://github.com/NASA-PDS/registry-sweepers/issues/91)) 


This requirement is not impacted by the current version
## As a user, I want to generate a report of all schema types that do not match their expected schema type per their LDD ([#128](https://github.com/NASA-PDS/registry-sweepers/issues/128)) 


This requirement is not impacted by the current version
## As a user, I want to force update all types that mismatch their expected types per their schema ([#129](https://github.com/NASA-PDS/registry-sweepers/issues/129)) 


This requirement is not impacted by the current version
## As an archivist, I want to validate that the number of records in a collection's metadata equals number of collection products loaded into registry ([#142](https://github.com/NASA-PDS/registry-sweepers/issues/142)) 


This requirement is not impacted by the current version
## As a developer, I want legacy sync sweeper run to be shortened in dev mode ([#153](https://github.com/NASA-PDS/registry-sweepers/issues/153)) 


This requirement is not impacted by the current version
## As a registry developer, I want to the keywords of related products to be available as secondary keyword in the primary product ([#158](https://github.com/NASA-PDS/registry-sweepers/issues/158)) 


This requirement is not impacted by the current version
## As a registry administrator, I want the system to enforce that only one sweeper can operate on a single node concurrently ([#203](https://github.com/NASA-PDS/registry-sweepers/issues/203)) 


This requirement is not impacted by the current version
## As a data user, I want to superseded products using PROV LDD metadata ([#206](https://github.com/NASA-PDS/registry-sweepers/issues/206)) 


This requirement is not impacted by the current version
## As a PDS cloud operator, I want to automatically deploy sweepers with specific docker commands ([#211](https://github.com/NASA-PDS/registry-sweepers/issues/211)) 


This requirement is not impacted by the current version
## As a user, I want a --force flag for legacy registry sync to overwrite existing node assignments ([#225](https://github.com/NASA-PDS/registry-sweepers/issues/225)) 


This requirement is not impacted by the current version
## Add --force flag to legacy registry sync to overwrite existing node assignments ([#226](https://github.com/NASA-PDS/registry-sweepers/issues/226)) 


This requirement is not impacted by the current version
## As a node operator, I want to run any individual sweeper by name from the command line ([#228](https://github.com/NASA-PDS/registry-sweepers/issues/228)) 


This requirement is not impacted by the current version
## As a node operator, I want `pds-registry-sweepers` available as an installed CLI command ([#229](https://github.com/NASA-PDS/registry-sweepers/issues/229)) 


This requirement is not impacted by the current version
## As a node operator, I want to specify the target OpenSearch endpoint and credentials as CLI arguments ([#230](https://github.com/NASA-PDS/registry-sweepers/issues/230)) 


This requirement is not impacted by the current version
## As a data user, I want to know when a bundle's member collections are all registered and linked ([#232](https://github.com/NASA-PDS/registry-sweepers/issues/232)) 


This requirement is not impacted by the current version
## As a data user, I want to know when a collection's member products are all registered and linked ([#233](https://github.com/NASA-PDS/registry-sweepers/issues/233)) 


This requirement is not impacted by the current version
## As a registry operator, I want each collection and bundle to record whether its ancestry is complete, so that sweepers process only incomplete ones and users can tell when membership is complete ([#243](https://github.com/NASA-PDS/registry-sweepers/issues/243)) 


This requirement is not impacted by the current version
## As a Registry administrator, I want to receive Alerts when one of the sweepers DAG fails ([#272](https://github.com/NASA-PDS/registry-sweepers/issues/272)) 


This requirement is not impacted by the current version
## As a registry operator, I want to force reprocess and re-stamp an entire index, regardless of existing sweeper version metadata ([#290](https://github.com/NASA-PDS/registry-sweepers/issues/290)) 


This requirement is not impacted by the current version
## As a registry operator, I want ancestry to be processed for a collection whose bundle is loaded after the collection was processed, so that the bundle's /members lists all of its collections ([#301](https://github.com/NASA-PDS/registry-sweepers/issues/301)) 


This requirement is not impacted by the current version
## As a data user, I want the ancestry sweeper to process Secondary members of a bundle, so that /members can include or distinguish them ([#306](https://github.com/NASA-PDS/registry-sweepers/issues/306)) 


This requirement is not impacted by the current version
## As a registry operator, I want ancestry to be processed for products that have ancestry for some of their collections but not all of them, so that every collection that lists a product is in its ancestry ([#308](https://github.com/NASA-PDS/registry-sweepers/issues/308)) 


This requirement is not impacted by the current version
## As a registry operator, I want sweepers to back off exponentially when OpenSearch Serverless blocks indexing for capacity, so that large runs complete ([#310](https://github.com/NASA-PDS/registry-sweepers/issues/310)) 


This requirement is not impacted by the current version