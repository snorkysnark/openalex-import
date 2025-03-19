--authors

COPY openalex.authors (id, orcid, display_name, display_name_alternatives, works_count, cited_by_count, last_known_institution, works_api_url, updated_date) FROM 'csv-files/authors.csv.gz';
COPY openalex.authors_ids (author_id, openalex, orcid, scopus, twitter, wikipedia, mag) FROM 'csv-files/authors_ids.csv.gz';
COPY openalex.authors_counts_by_year (author_id, year, works_count, cited_by_count, oa_works_count) FROM 'csv-files/authors_counts_by_year.csv.gz';

-- topics

COPY openalex.topics (id, display_name, subfield_id, subfield_display_name, field_id, field_display_name, domain_id, domain_display_name, description, keywords, works_api_url, wikipedia_id, works_count, cited_by_count, updated_date, siblings) FROM 'csv-files/topics.csv.gz';

--concepts

COPY openalex.concepts (id, wikidata, display_name, level, description, works_count, cited_by_count, image_url, image_thumbnail_url, works_api_url, updated_date) FROM 'csv-files/concepts.csv.gz';
COPY openalex.concepts_ancestors (concept_id, ancestor_id) FROM 'csv-files/concepts_ancestors.csv.gz';
COPY openalex.concepts_counts_by_year (concept_id, year, works_count, cited_by_count, oa_works_count) FROM 'csv-files/concepts_counts_by_year.csv.gz';
COPY openalex.concepts_ids (concept_id, openalex, wikidata, wikipedia, umls_aui, umls_cui, mag) FROM 'csv-files/concepts_ids.csv.gz';
COPY openalex.concepts_related_concepts (concept_id, related_concept_id, score) FROM 'csv-files/concepts_related_concepts.csv.gz';

--institutions

COPY openalex.institutions (id, ror, display_name, country_code, type, homepage_url, image_url, image_thumbnail_url, display_name_acronyms, display_name_alternatives, works_count, cited_by_count, works_api_url, updated_date) FROM 'csv-files/institutions.csv.gz';
COPY openalex.institutions_ids (institution_id, openalex, ror, grid, wikipedia, wikidata, mag) FROM 'csv-files/institutions_ids.csv.gz';
COPY openalex.institutions_geo (institution_id, city, geonames_city_id, region, country_code, country, latitude, longitude) FROM 'csv-files/institutions_geo.csv.gz';
COPY openalex.institutions_associated_institutions (institution_id, associated_institution_id, relationship) FROM 'csv-files/institutions_associated_institutions.csv.gz';
COPY openalex.institutions_counts_by_year (institution_id, year, works_count, cited_by_count, oa_works_count) FROM 'csv-files/institutions_counts_by_year.csv.gz';

--publishers

COPY openalex.publishers (id, display_name, alternate_titles, country_codes, hierarchy_level, parent_publisher, works_count, cited_by_count, sources_api_url, updated_date) FROM 'csv-files/publishers.csv.gz';
COPY openalex.publishers_ids (publisher_id, openalex, ror, wikidata) FROM 'csv-files/publishers_ids.csv.gz';
COPY openalex.publishers_counts_by_year (publisher_id, year, works_count, cited_by_count, oa_works_count) FROM 'csv-files/publishers_counts_by_year.csv.gz';

--sources

COPY openalex.sources (id, issn_l, issn, display_name, publisher, works_count, cited_by_count, is_oa, is_in_doaj, homepage_url, works_api_url, updated_date) FROM 'csv-files/sources.csv.gz';
COPY openalex.sources_ids (source_id, openalex, issn_l, issn, mag, wikidata, fatcat) FROM 'csv-files/sources_ids.csv.gz';
COPY openalex.sources_counts_by_year (source_id, year, works_count, cited_by_count, oa_works_count) FROM 'csv-files/sources_counts_by_year.csv.gz';

--works

COPY openalex.works (id, doi, title, display_name, publication_year, publication_date, type, cited_by_count, is_retracted, is_paratext, cited_by_api_url, abstract_inverted_index, language) FROM 'csv-files/works.csv.gz';
COPY openalex.works_primary_locations (work_id, source_id, landing_page_url, pdf_url, is_oa, version, license) FROM 'csv-files/works_primary_locations.csv.gz';
COPY openalex.works_locations (work_id, source_id, landing_page_url, pdf_url, is_oa, version, license) FROM 'csv-files/works_locations.csv.gz';
COPY openalex.works_best_oa_locations (work_id, source_id, landing_page_url, pdf_url, is_oa, version, license) FROM 'csv-files/works_best_oa_locations.csv.gz';
COPY openalex.works_authorships (work_id, author_position, author_id, institution_id, raw_affiliation_string) FROM 'csv-files/works_authorships.csv.gz';
COPY openalex.works_biblio (work_id, volume, issue, first_page, last_page) FROM 'csv-files/works_biblio.csv.gz';
COPY openalex.works_topics (work_id, topic_id, score) FROM 'csv-files/works_topics.csv.gz';
COPY openalex.works_concepts (work_id, concept_id, score) FROM 'csv-files/works_concepts.csv.gz';
COPY openalex.works_ids (work_id, openalex, doi, mag, pmid, pmcid) FROM 'csv-files/works_ids.csv.gz';
COPY openalex.works_mesh (work_id, descriptor_ui, descriptor_name, qualifier_ui, qualifier_name, is_major_topic) FROM 'csv-files/works_mesh.csv.gz';
COPY openalex.works_open_access (work_id, is_oa, oa_status, oa_url, any_repository_has_fulltext) FROM 'csv-files/works_open_access.csv.gz';
COPY openalex.works_referenced_works (work_id, referenced_work_id) FROM 'csv-files/works_referenced_works.csv.gz';
COPY openalex.works_related_works (work_id, related_work_id) FROM 'csv-files/works_related_works.csv.gz';
