import glob
import gzip
import json
from pathlib import Path
from typing import Annotated
from sqlalchemy import Connection, Table, MetaData, Column, create_engine
import typer

_metadata = MetaData(schema="openalex")
table_authors = Table(
    "authors",
    _metadata,
    Column("id"),
    Column("orcid"),
    Column("display_name"),
    Column("display_name_alternatives"),
    Column("works_count"),
    Column("cited_by_count"),
    Column("last_known_institution"),
    Column("works_api_url"),
    Column("updated_date"),
)
table_author_ids = Table(
    "authors_ids",
    _metadata,
    Column("author_id"),
    Column("openalex"),
    Column("orcid"),
    Column("scopus"),
    Column("twitter"),
    Column("wikipedia"),
    Column("mag"),
)
table_counts_by_year = Table(
    "authors_counts_by_year",
    _metadata,
    Column("author_id"),
    Column("year"),
    Column("works_count"),
    Column("cited_by_count"),
    Column("oa_works_count"),
)
table_topics = Table(
    "topics",
    _metadata,
    Column("id"),
    Column("display_name"),
    Column("subfield_id"),
    Column("subfield_display_name"),
    Column("field_id"),
    Column("field_display_name"),
    Column("domain_id"),
    Column("domain_display_name"),
    Column("description"),
    Column("keywords"),
    Column("works_api_url"),
    Column("wikipedia_id"),
    Column("works_count"),
    Column("cited_by_count"),
    Column("updated_date"),
    Column("siblings"),
)
table_concepts = Table(
    "concepts",
    _metadata,
    Column("id"),
    Column("wikidata"),
    Column("display_name"),
    Column("level"),
    Column("description"),
    Column("works_count"),
    Column("cited_by_count"),
    Column("image_url"),
    Column("image_thumbnail_url"),
    Column("works_api_url"),
    Column("updated_date"),
)
table_concepts_ancestors = Table(
    "concepts_ancestors", _metadata, Column("concept_id"), Column("ancestor_id")
)
table_concepts_counts_by_year = Table(
    "concepts_counts_by_year",
    _metadata,
    Column("concept_id"),
    Column("year"),
    Column("works_count"),
    Column("cited_by_count"),
    Column("oa_works_count"),
)
table_concepts_ids = Table(
    "concepts_ids",
    _metadata,
    Column("concept_id"),
    Column("openalex"),
    Column("wikidata"),
    Column("wikipedia"),
    Column("umls_aui"),
    Column("umls_cui"),
    Column("mag"),
)
# concept_id, related_concept_id, score
table_concepts_related_concepts = Table(
    "concepts_related_concepts",
    _metadata,
    Column("concept_id"),
    Column("related_concept_id"),
    Column("score"),
)
# id, ror, display_name, country_code, type, homepage_url, image_url, image_thumbnail_url, display_name_acronyms, display_name_alternatives, works_count, cited_by_count, works_api_url, updated_date
table_institutions = Table(
    "institutions",
    _metadata,
    Column("id"),
    Column("ror"),
    Column("display_name"),
    Column("country_code"),
    Column("type"),
    Column("homepage_url"),
    Column("image_url"),
    Column("image_thumbnail_url"),
    Column("display_name_acronyms"),
    Column("display_name_alternatives"),
    Column("works_count"),
    Column("cited_by_count"),
    Column("works_api_url"),
    Column("updated_date"),
)
# institution_id, openalex, ror, grid, wikipedia, wikidata, mag
table_institutions_ids = Table(
    "institutions_ids",
    _metadata,
    Column("institution_id"),
    Column("openalex"),
    Column("ror"),
    Column("grid"),
    Column("wikipedia"),
    Column("wikidata"),
    Column("mag"),
)
# institution_id, city, geonames_city_id, region, country_code, country, latitude, longitude
table_institutions_geo = Table(
    "institutions_geo",
    _metadata,
    Column("institution_id"),
    Column("city"),
    Column("geonames_city_id"),
    Column("region"),
    Column("country_code"),
    Column("country"),
    Column("latitude"),
    Column("longitude"),
)
# institution_id, associated_institution_id, relationship
table_institutions_associated_institutions = Table(
    "institutions_associated_institutions",
    _metadata,
    Column("institution_id"),
    Column("associated_institution_id"),
    Column("relationship"),
)
# institution_id, year, works_count, cited_by_count, oa_works_count
table_institutions_counts_by_year = Table(
    "institutions_counts_by_year",
    _metadata,
    Column("institution_id"),
    Column("year"),
    Column("works_count"),
    Column("cited_by_count"),
    Column("oa_works_count"),
)
# id, display_name, alternate_titles, country_codes, hierarchy_level, parent_publisher, works_count, cited_by_count, sources_api_url, updated_date
table_publishers = Table(
    "publishers",
    _metadata,
    Column("id"),
    Column("display_name"),
    Column("alternate_titles"),
    Column("country_codes"),
    Column("hierarchy_level"),
    Column("parent_publisher"),
    Column("works_count"),
    Column("cited_by_count"),
    Column("sources_api_url"),
    Column("updated_date"),
)
# publisher_id, year, works_count, cited_by_count, oa_works_count
table_publishers_counts_by_year = Table(
    "publishers_counts_by_year",
    _metadata,
    Column("publisher_id"),
    Column("year"),
    Column("works_count"),
    Column("cited_by_count"),
    Column("oa_works_count"),
)
# publisher_id, openalex, ror, wikidata
table_publishers_ids = Table(
    "publishers_ids",
    _metadata,
    Column("publisher_id"),
    Column("openalex"),
    Column("ror"),
    Column("wikidata"),
)
# id, issn_l, issn, display_name, publisher, works_count, cited_by_count, is_oa, is_in_doaj, homepage_url, works_api_url, updated_date
table_sources = Table(
    "sources",
    _metadata,
    Column("id"),
    Column("issn_l"),
    Column("issn"),
    Column("display_name"),
    Column("publisher"),
    Column("works_count"),
    Column("cited_by_count"),
    Column("is_oa"),
    Column("is_in_doaj"),
    Column("homepage_url"),
    Column("works_api_url"),
    Column("updated_date"),
)
# source_id, openalex, issn_l, issn, mag, wikidata, fatcat
table_sources_ids = Table(
    "sources_ids",
    _metadata,
    Column("source_id"),
    Column("openalex"),
    Column("issn_l"),
    Column("issn"),
    Column("mag"),
    Column("wikidata"),
    Column("fatcat"),
)
# source_id, year, works_count, cited_by_count, oa_works_count
table_sources_counts_by_year = Table(
    "sources_counts_by_year",
    _metadata,
    Column("source_id"),
    Column("year"),
    Column("works_count"),
    Column("cited_by_count"),
    Column("oa_works_count"),
)
# COPY openalex.works (id, doi, title, display_name, publication_year, publication_date, type, cited_by_count, is_retracted, is_paratext, cited_by_api_url, abstract_inverted_index, language) FROM 'csv-files/works.csv.gz';
table_works = Table(
    "works",
    _metadata,
    Column("id"),
    Column("doi"),
    Column("title"),
    Column("display_name"),
    Column("publication_year"),
    Column("publication_date"),
    Column("type"),
    Column("cited_by_count"),
    Column("is_retracted"),
    Column("is_paratext"),
    Column("cited_by_api_url"),
    Column("abstract_inverted_index"),
    Column("language"),
)
# COPY openalex.works_primary_locations (work_id, source_id, landing_page_url, pdf_url, is_oa, version, license) FROM 'csv-files/works_primary_locations.csv.gz';
table_works_primary_locations = Table(
    "works_primary_locations",
    _metadata,
    Column("work_id"),
    Column("source_id"),
    Column("landing_page_url"),
    Column("pdf_url"),
    Column("is_oa"),
    Column("version"),
    Column("license"),
)
# COPY openalex.works_locations (work_id, source_id, landing_page_url, pdf_url, is_oa, version, license) FROM 'csv-files/works_locations.csv.gz';
table_works_locations = Table(
    "works_locations",
    _metadata,
    Column("work_id"),
    Column("source_id"),
    Column("landing_page_url"),
    Column("pdf_url"),
    Column("is_oa"),
    Column("version"),
    Column("license"),
)
# COPY openalex.works_best_oa_locations (work_id, source_id, landing_page_url, pdf_url, is_oa, version, license) FROM 'csv-files/works_best_oa_locations.csv.gz';
table_works_best_oa_locations = Table(
    "works_best_oa_locations",
    _metadata,
    Column("work_id"),
    Column("source_id"),
    Column("landing_page_url"),
    Column("pdf_url"),
    Column("is_oa"),
    Column("version"),
    Column("license"),
)
# COPY openalex.works_authorships (work_id, author_position, author_id, institution_id, raw_affiliation_string) FROM 'csv-files/works_authorships.csv.gz';
table_works_authorships = Table(
    "works_authorships",
    _metadata,
    Column("work_id"),
    Column("author_position"),
    Column("author_id"),
    Column("institution_id"),
    Column("raw_affiliation_string"),
)
# COPY openalex.works_biblio (work_id, volume, issue, first_page, last_page) FROM 'csv-files/works_biblio.csv.gz';
table_works_biblio = Table(
    "works_biblio",
    _metadata,
    Column("work_id"),
    Column("volume"),
    Column("issue"),
    Column("first_page"),
    Column("last_page"),
)
# COPY openalex.works_topics (work_id, topic_id, score) FROM 'csv-files/works_topics.csv.gz';
table_works_topics = Table(
    "works_topics", _metadata, Column("work_id"), Column("topic_id"), Column("score")
)
# COPY openalex.works_concepts (work_id, concept_id, score) FROM 'csv-files/works_concepts.csv.gz';
table_works_concepts = Table(
    "works_concepts",
    _metadata,
    Column("work_id"),
    Column("concept_id"),
    Column("score"),
)
# COPY openalex.works_ids (work_id, openalex, doi, mag, pmid, pmcid) FROM 'csv-files/works_ids.csv.gz';
table_works_ids = Table(
    "works_ids",
    _metadata,
    Column("work_id"),
    Column("openalex"),
    Column("doi"),
    Column("mag"),
    Column("pmid"),
    Column("pmcid"),
)
# COPY openalex.works_mesh (work_id, descriptor_ui, descriptor_name, qualifier_ui, qualifier_name, is_major_topic) FROM 'csv-files/works_mesh.csv.gz';
table_works_mesh = Table(
    "works_mesh",
    _metadata,
    Column("work_id"),
    Column("descriptor_ui"),
    Column("descriptor_name"),
    Column("qualifier_ui"),
    Column("qualifier_name"),
    Column("is_major_topic"),
)
# COPY openalex.works_open_access (work_id, is_oa, oa_status, oa_url, any_repository_has_fulltext) FROM 'csv-files/works_open_access.csv.gz';
table_works_open_access = Table(
    "works_open_access",
    _metadata,
    Column("work_id"),
    Column("is_oa"),
    Column("oa_status"),
    Column("oa_url"),
    Column("any_repository_has_fulltext"),
)
# COPY openalex.works_referenced_works (work_id, referenced_work_id) FROM 'csv-files/works_referenced_works.csv.gz';
table_works_referenced_works = Table(
    "works_referenced_works", _metadata, Column("work_id"), Column("referenced_work_id")
)
# COPY openalex.works_related_works (work_id, related_work_id) FROM 'csv-files/works_related_works.csv.gz';
table_works_related_works = Table(
    "works_related_works", _metadata, Column("work_id"), Column("related_work_id")
)


def load_authors(snapshot_dir: Path, conn: Connection):
    for jsonl_file_name in glob.glob(
        str(snapshot_dir.joinpath("data", "authors", "*", "*.gz"))
    ):
        with gzip.open(jsonl_file_name, "r") as authors_jsonl:
            for author_json in authors_jsonl:
                if not author_json.strip():
                    continue

                author = json.loads(author_json)

                if not (author_id := author.get("id")):
                    continue

                # authors
                author["display_name_alternatives"] = json.dumps(
                    author.get("display_name_alternatives"), ensure_ascii=False
                )
                author["last_known_institution"] = (
                    author.get("last_known_institution") or {}
                ).get("id")

                conn.execute(table_authors.insert(), author)

                # ids
                if author_ids := author.get("ids"):
                    author_ids["author_id"] = author_id
                    conn.execute(table_author_ids.insert(), author_ids)

                # counts_by_year
                if counts_by_year := author.get("counts_by_year"):
                    for count_by_year in counts_by_year:
                        count_by_year["author_id"] = author_id
                        conn.execute(table_counts_by_year.insert(), count_by_year)


def load_topics(snapshot_dir: Path, conn: Connection):
    seen_topic_ids = set()
    for jsonl_file_name in glob.glob(
        str(snapshot_dir.joinpath("data", "topics", "*", "*.gz"))
    ):
        print(jsonl_file_name)
        with gzip.open(jsonl_file_name, "r") as topics_jsonl:
            for line in topics_jsonl:
                if not line.strip():
                    continue
                topic = json.loads(line)
                topic["keywords"] = "; ".join(topic.get("keywords", ""))
                if not (topic_id := topic.get("id")) or topic_id in seen_topic_ids:
                    continue
                seen_topic_ids.add(topic_id)
                for key in ("subfield", "field", "domain"):
                    topic[f"{key}_id"] = topic[key]["id"]
                    topic[f"{key}_display_name"] = topic[key]["display_name"]
                    del topic[key]

                if "updated" in topic:
                    topic["updated_date"] = topic["updated"]
                    del topic["updated"]

                topic["wikipedia_id"] = topic["ids"].get("wikipedia")
                del topic["ids"]
                del topic["created_date"]

                topic["siblings"] = json.dumps(
                    topic.get("siblings"), ensure_ascii=False
                )
                conn.execute(table_topics.insert(), topic)


def load_concepts(snapshot_dir: Path, conn: Connection):
    seen_concept_ids = set()

    for jsonl_file_name in glob.glob(
        str(snapshot_dir.joinpath("data", "concepts", "*", "*.gz"))
    ):
        print(jsonl_file_name)
        with gzip.open(jsonl_file_name, "r") as concepts_jsonl:
            for concept_json in concepts_jsonl:
                if not concept_json.strip():
                    continue

                concept = json.loads(concept_json)

                if (
                    not (concept_id := concept.get("id"))
                    or concept_id in seen_concept_ids
                ):
                    continue

                seen_concept_ids.add(concept_id)

                conn.execute(table_concepts.insert(), concept)

                if concept_ids := concept.get("ids"):
                    concept_ids["concept_id"] = concept_id
                    concept_ids["umls_aui"] = json.dumps(
                        concept_ids.get("umls_aui"), ensure_ascii=False
                    )
                    concept_ids["umls_cui"] = json.dumps(
                        concept_ids.get("umls_cui"), ensure_ascii=False
                    )
                    conn.execute(table_concepts_ids.insert(), concept_ids)

                if ancestors := concept.get("ancestors"):
                    for ancestor in ancestors:
                        if ancestor_id := ancestor.get("id"):
                            conn.execute(
                                table_concepts_ancestors.insert(),
                                {"concept_id": concept_id, "ancestor_id": ancestor_id},
                            )

                if counts_by_year := concept.get("counts_by_year"):
                    for count_by_year in counts_by_year:
                        count_by_year["concept_id"] = concept_id
                        conn.execute(
                            table_concepts_counts_by_year.insert(), count_by_year
                        )

                if related_concepts := concept.get("related_concepts"):
                    for related_concept in related_concepts:
                        if related_concept_id := related_concept.get("id"):
                            conn.execute(
                                table_concepts_related_concepts.insert(),
                                {
                                    "concept_id": concept_id,
                                    "related_concept_id": related_concept_id,
                                    "score": related_concept.get("score"),
                                },
                            )


def load_institutions(snapshot_dir: Path, conn: Connection):
    seen_institution_ids = set()

    for jsonl_file_name in glob.glob(
        str(snapshot_dir.joinpath("data", "institutions", "*", "*.gz"))
    ):
        print(jsonl_file_name)
        with gzip.open(jsonl_file_name, "r") as institutions_jsonl:
            for institution_json in institutions_jsonl:
                if not institution_json.strip():
                    continue

                institution = json.loads(institution_json)

                if (
                    not (institution_id := institution.get("id"))
                    or institution_id in seen_institution_ids
                ):
                    continue

                seen_institution_ids.add(institution_id)

                # institutions
                institution["display_name_acronyms"] = json.dumps(
                    institution.get("display_name_acronyms"), ensure_ascii=False
                )
                institution["display_name_alternatives"] = json.dumps(
                    institution.get("display_name_alternatives"), ensure_ascii=False
                )
                conn.execute(table_institutions.insert(), institution)

                # ids
                if institution_ids := institution.get("ids"):
                    institution_ids["institution_id"] = institution_id
                    conn.execute(table_institutions_ids.insert(), institution_ids)

                # geo
                if institution_geo := institution.get("geo"):
                    institution_geo["institution_id"] = institution_id
                    conn.execute(table_institutions_geo.insert(), institution_geo)

                # associated_institutions
                if associated_institutions := institution.get(
                    "associated_institutions",
                    institution.get("associated_insitutions"),
                    # typo in api
                ):
                    for associated_institution in associated_institutions:
                        if associated_institution_id := associated_institution.get(
                            "id"
                        ):
                            conn.execute(
                                table_institutions_associated_institutions.insert(),
                                {
                                    "institution_id": institution_id,
                                    "associated_institution_id": associated_institution_id,
                                    "relationship": associated_institution.get(
                                        "relationship"
                                    ),
                                },
                            )

                # counts_by_year
                if counts_by_year := institution.get("counts_by_year"):
                    for count_by_year in counts_by_year:
                        count_by_year["institution_id"] = institution_id
                        conn.execute(
                            table_institutions_counts_by_year.insert(), count_by_year
                        )


def load_publishers(snapshot_dir: Path, conn: Connection):
    seen_publisher_ids = set()

    for jsonl_file_name in glob.glob(
        str(snapshot_dir.joinpath("data", "publishers", "*", "*.gz"))
    ):
        print(jsonl_file_name)
        with gzip.open(jsonl_file_name, "r") as concepts_jsonl:
            for publisher_json in concepts_jsonl:
                if not publisher_json.strip():
                    continue

                publisher = json.loads(publisher_json)

                if (
                    not (publisher_id := publisher.get("id"))
                    or publisher_id in seen_publisher_ids
                ):
                    continue

                seen_publisher_ids.add(publisher_id)

                # publishers
                publisher["alternate_titles"] = json.dumps(
                    publisher.get("alternate_titles"), ensure_ascii=False
                )
                publisher["country_codes"] = json.dumps(
                    publisher.get("country_codes"), ensure_ascii=False
                )
                conn.execute(table_publishers.insert(), publisher)

                if publisher_ids := publisher.get("ids"):
                    publisher_ids["publisher_id"] = publisher_id
                    conn.execute(table_publishers_ids.insert(), publisher_ids)

                if counts_by_year := publisher.get("counts_by_year"):
                    for count_by_year in counts_by_year:
                        count_by_year["publisher_id"] = publisher_id
                        conn.execute(
                            table_publishers_counts_by_year.insert(), count_by_year
                        )


def load_sources(snapshot_dir: Path, conn: Connection):
    seen_source_ids = set()
    for jsonl_file_name in glob.glob(
        str(snapshot_dir.joinpath("data", "sources", "*", "*.gz"))
    ):
        print(jsonl_file_name)
        with gzip.open(jsonl_file_name, "r") as sources_jsonl:
            for source_json in sources_jsonl:
                if not source_json.strip():
                    continue

                source = json.loads(source_json)

                if not (source_id := source.get("id")) or source_id in seen_source_ids:
                    continue

                seen_source_ids.add(source_id)

                source["issn"] = json.dumps(source.get("issn"))
                conn.execute(table_sources.insert(), source)

                if source_ids := source.get("ids"):
                    source_ids["source_id"] = source_id
                    source_ids["issn"] = json.dumps(source_ids.get("issn"))
                    conn.execute(table_sources_ids.insert(), source_ids)

                if counts_by_year := source.get("counts_by_year"):
                    for count_by_year in counts_by_year:
                        count_by_year["source_id"] = source_id
                        conn.execute(
                            table_sources_counts_by_year.insert(), count_by_year
                        )


def load_works(snapshot_dir: Path, conn: Connection):
    for jsonl_file_name in glob.glob(
        str(snapshot_dir.joinpath("data", "works", "*", "*.gz"))
    ):
        print(jsonl_file_name)
        with gzip.open(jsonl_file_name, "r") as works_jsonl:
            for work_json in works_jsonl:
                if not work_json.strip():
                    continue

                work = json.loads(work_json)

                if not (work_id := work.get("id")):
                    continue

                # works
                if (abstract := work.get("abstract_inverted_index")) is not None:
                    work["abstract_inverted_index"] = json.dumps(
                        abstract, ensure_ascii=False
                    )

                conn.execute(table_works.insert(), work)

                # primary_locations
                if primary_location := (work.get("primary_location") or {}):
                    if primary_location.get("source") and primary_location[
                        "source"
                    ].get("id"):
                        conn.execute(
                            table_works_primary_locations.insert(),
                            {
                                "work_id": work_id,
                                "source_id": primary_location["source"]["id"],
                                "landing_page_url": primary_location.get(
                                    "landing_page_url"
                                ),
                                "pdf_url": primary_location.get("pdf_url"),
                                "is_oa": primary_location.get("is_oa"),
                                "version": primary_location.get("version"),
                                "license": primary_location.get("license"),
                            },
                        )

                # locations
                if locations := work.get("locations"):
                    for location in locations:
                        if location.get("source") and location.get("source").get("id"):
                            conn.execute(
                                table_works_locations.insert(),
                                {
                                    "work_id": work_id,
                                    "source_id": location["source"]["id"],
                                    "landing_page_url": location.get(
                                        "landing_page_url"
                                    ),
                                    "pdf_url": location.get("pdf_url"),
                                    "is_oa": location.get("is_oa"),
                                    "version": location.get("version"),
                                    "license": location.get("license"),
                                },
                            )

                # best_oa_locations
                if best_oa_location := (work.get("best_oa_location") or {}):
                    if best_oa_location.get("source") and best_oa_location[
                        "source"
                    ].get("id"):
                        conn.execute(
                            table_works_best_oa_locations.insert(),
                            {
                                "work_id": work_id,
                                "source_id": best_oa_location["source"]["id"],
                                "landing_page_url": best_oa_location.get(
                                    "landing_page_url"
                                ),
                                "pdf_url": best_oa_location.get("pdf_url"),
                                "is_oa": best_oa_location.get("is_oa"),
                                "version": best_oa_location.get("version"),
                                "license": best_oa_location.get("license"),
                            },
                        )

                # authorships
                if authorships := work.get("authorships"):
                    for authorship in authorships:
                        if author_id := authorship.get("author", {}).get("id"):
                            institutions = authorship.get("institutions")
                            institution_ids = [i.get("id") for i in institutions]
                            institution_ids = [i for i in institution_ids if i]
                            institution_ids = institution_ids or [None]

                            for institution_id in institution_ids:
                                conn.execute(
                                    table_works_authorships.insert(),
                                    {
                                        "work_id": work_id,
                                        "author_position": authorship.get(
                                            "author_position"
                                        ),
                                        "author_id": author_id,
                                        "institution_id": institution_id,
                                        "raw_affiliation_string": authorship.get(
                                            "raw_affiliation_string"
                                        ),
                                    },
                                )

                # biblio
                if biblio := work.get("biblio"):
                    biblio["work_id"] = work_id
                    conn.execute(table_works_biblio.insert(), biblio)

                # topics
                for topic in work.get("topics", []):
                    if topic_id := topic.get("id"):
                        conn.execute(
                            table_works_topics.insert(),
                            {
                                "work_id": work_id,
                                "topic_id": topic_id,
                                "score": topic.get("score"),
                            },
                        )

                # concepts
                for concept in work.get("concepts"):
                    if concept_id := concept.get("id"):
                        conn.execute(
                            table_works_concepts.insert(),
                            {
                                "work_id": work_id,
                                "concept_id": concept_id,
                                "score": concept.get("score"),
                            },
                        )

                # ids
                if ids := work.get("ids"):
                    ids["work_id"] = work_id
                    conn.execute(table_works_ids.insert(), ids)

                # mesh
                for mesh in work.get("mesh"):
                    mesh["work_id"] = work_id
                    conn.execute(table_works_mesh.insert(), mesh)

                # open_access
                if open_access := work.get("open_access"):
                    open_access["work_id"] = work_id
                    conn.execute(table_works_open_access.insert(), open_access)

                # referenced_works
                for referenced_work in work.get("referenced_works"):
                    if referenced_work:
                        conn.execute(
                            table_works_referenced_works.insert(),
                            {
                                "work_id": work_id,
                                "referenced_work_id": referenced_work,
                            },
                        )

                # related_works
                for related_work in work.get("related_works"):
                    if related_work:
                        conn.execute(
                            table_works_related_works.insert(),
                            {"work_id": work_id, "related_work_id": related_work},
                        )


def main(
    snapshot_dir: Path,
    db_url: str,
    echo: Annotated[bool, typer.Option(help="echo sqlalchemy statements")] = False,
):
    with create_engine(db_url, echo=echo).connect() as conn:
        load_topics(snapshot_dir, conn)
        load_authors(snapshot_dir, conn)
        load_concepts(snapshot_dir, conn)
        load_institutions(snapshot_dir, conn)
        load_publishers(snapshot_dir, conn)
        load_sources(snapshot_dir, conn)
        load_works(snapshot_dir, conn)

        conn.commit()


if __name__ == "__main__":
    typer.run(main)
