from sqlalchemy import (
    BigInteger, Boolean, CHAR, Column, Date, DateTime, Float,
    Index, Integer, MetaData, Numeric, SmallInteger, String, Table, Text,
)

# ---------------------------------------------------------------------------
# Schema — table definitions via SQLAlchemy Core
# ---------------------------------------------------------------------------

metadata = MetaData()

t_geoname = Table(
    "geoname", metadata,
    Column("geonameid",      Integer,      nullable=True),
    Column("name",           String(200),  nullable=True),
    Column("asciiname",      String(200),  nullable=True),
    Column("alternatenames", Text,         nullable=True),
    Column("latitude",       Float,        nullable=True),
    Column("longitude",      Float,        nullable=True),
    Column("fclass",         CHAR(1),      nullable=True),
    Column("fcode",          String(10),   nullable=True),
    Column("country",        String(3),    nullable=True),
    Column("cc2",            Text,         nullable=True),
    Column("admin1",         String(20),   nullable=True),
    Column("admin2",         String(80),   nullable=True),
    Column("admin3",         String(20),   nullable=True),
    Column("admin4",         String(20),   nullable=True),
    Column("population",     BigInteger,   nullable=True),
    Column("elevation",      Integer,      nullable=True),
    Column("gtopo30",        Integer,      nullable=True),
    Column("timezone",       String(40),   nullable=True),
    Column("moddate",        Date,         nullable=True),
)

t_alternatename = Table(
    "alternatename", metadata,
    Column("alternatenameid", Integer,      nullable=True),
    Column("geonameid",       Integer,      nullable=True),
    Column("isolanguage",     String(7),    nullable=True),
    Column("alternatename",   String(500),  nullable=True),
    Column("ispreferredname", Boolean,      nullable=True),
    Column("isshortname",     Boolean,      nullable=True),
    Column("iscolloquial",    Boolean,      nullable=True),
    Column("ishistoric",      Boolean,      nullable=True),
)

t_countryinfo = Table(
    "countryinfo", metadata,
    Column("iso_alpha2",           CHAR(2),     nullable=True),
    Column("iso_alpha3",           CHAR(3),     nullable=True),
    Column("iso_numeric",          Integer,     nullable=True),
    Column("fips_code",            String(3),   nullable=True),
    Column("country",              String(200), nullable=True),
    Column("capital",              String(200), nullable=True),
    Column("areainsqkm",           Float,       nullable=True),
    Column("population",           Integer,     nullable=True),
    Column("continent",            CHAR(3),     nullable=True),
    Column("tld",                  CHAR(10),    nullable=True),
    Column("currency_code",        CHAR(3),     nullable=True),
    Column("currency_name",        CHAR(25),    nullable=True),
    Column("phone",                String(20),  nullable=True),
    Column("postal",               String(60),  nullable=True),
    Column("postalregex",          String(200), nullable=True),
    Column("languages",            String(200), nullable=True),
    Column("geonameid",            Integer,     nullable=True),
    Column("neighbours",           String(50),  nullable=True),
    Column("equivalent_fips_code", String(3),   nullable=True),
)

t_iso_languagecodes = Table(
    "iso_languagecodes", metadata,
    Column("iso_639_3",     CHAR(4),     nullable=True),
    Column("iso_639_2",     String(50),  nullable=True),
    Column("iso_639_1",     String(50),  nullable=True),
    Column("language_name", String(200), nullable=True),
)

t_admin1codesascii = Table(
    "admin1codesascii", metadata,
    Column("code",        CHAR(20),   nullable=True),
    Column("name",        Text,       nullable=True),
    Column("nameascii",   Text,       nullable=True),
    Column("geonameid",   Integer,    nullable=True),
    # Derived: first 2 chars of code (populated during enrichment)
    Column("countrycode", String(25), nullable=True),
)

t_admin2codesascii = Table(
    "admin2codesascii", metadata,
    Column("code",        CHAR(80),   nullable=True),
    Column("name",        Text,       nullable=True),
    Column("nameascii",   Text,       nullable=True),
    Column("geonameid",   Integer,    nullable=True),
    # Derived: first 2 chars of code (populated during enrichment)
    Column("countrycode", String(25), nullable=True),
)

t_featurecodes = Table(
    "featurecodes", metadata,
    Column("code",        CHAR(7),     nullable=True),
    Column("name",        String(200), nullable=True),
    Column("description", Text,        nullable=True),
)

t_timezones = Table(
    "timezones", metadata,
    Column("countrycode", CHAR(20),       nullable=True),
    Column("timezoneid",  String(200),    nullable=True),
    Column("gmt_offset",  Numeric(3, 1),  nullable=True),
    Column("dst_offset",  Numeric(3, 1),  nullable=True),
    Column("raw_offset",  Numeric(3, 1),  nullable=True),
)

t_continentcodes = Table(
    "continentcodes", metadata,
    Column("code",      CHAR(2),    nullable=True),
    Column("name",      String(20), nullable=True),
    Column("geonameid", Integer,    nullable=True),
)

t_postalcodes = Table(
    "postalcodes", metadata,
    Column("countrycode",     CHAR(2),      nullable=True),
    Column("postalcode",      String(20),   nullable=True),
    Column("placename",       String(180),  nullable=True),
    Column("admin1name",      String(100),  nullable=True),
    Column("admin1code",      String(20),   nullable=True),
    Column("admin2name",      String(100),  nullable=True),
    Column("admin2code",      String(20),   nullable=True),
    Column("admin3name",      String(100),  nullable=True),
    Column("admin3code",      String(20),   nullable=True),
    Column("latitude",        Float,        nullable=True),
    Column("longitude",       Float,        nullable=True),
    Column("accuracy",        SmallInteger, nullable=True),
    # Derived columns (populated during enrichment)
    Column("admin1code_full", String(100),  nullable=True),
    Column("admin2code_full", String(100),  nullable=True),
    Column("admin3code_full", String(100),  nullable=True),
    Column("admin1nameascii", String(100),  nullable=True),
    Column("admin2nameascii", String(100),  nullable=True),
    Column("admin3nameascii", String(100),  nullable=True),
)

t_meta = Table(
    "meta", metadata,
    Column("version",       Text,     nullable=True),
    Column("data_uri",      Text,     nullable=True),
    Column("data_version",  Text,     nullable=True),
    Column("date_accessed", DateTime, nullable=True),
)

# ---------------------------------------------------------------------------
# Indexes (created after bulk load for performance)
# ---------------------------------------------------------------------------

indexes = [
    # countryinfo
    Index("countryinfo_geonameid_idx",             t_countryinfo.c.geonameid),
    # alternatename
    Index("alternatename_geonameid_idx",            t_alternatename.c.geonameid),
    Index("alternatename_isolanguage_idx",          t_alternatename.c.isolanguage),
    Index("alternatename_alternatename_idx",        t_alternatename.c.alternatename),
    Index("alternatename_ispreferredname_idx",      t_alternatename.c.ispreferredname),
    Index("alternatename_isshortname_idx",          t_alternatename.c.isshortname),
    Index("alternatename_iscolloquial_idx",         t_alternatename.c.iscolloquial),
    Index("alternatename_ishistoric_idx",           t_alternatename.c.ishistoric),
    # geoname
    Index("geoname_name_idx",                      t_geoname.c.name),
    Index("geoname_asciiname_idx",                 t_geoname.c.asciiname),
    Index("geoname_fclass_idx",                    t_geoname.c.fclass),
    Index("geoname_fcode_idx",                     t_geoname.c.fcode),
    Index("geoname_country_idx",                   t_geoname.c.country),
    Index("geoname_cc2_idx",                       t_geoname.c.cc2),
    Index("geoname_admin1_idx",                    t_geoname.c.admin1),
    Index("geoname_admin2_idx",                    t_geoname.c.admin2),
    Index("geoname_admin3_idx",                    t_geoname.c.admin3),
    Index("geoname_admin4_idx",                    t_geoname.c.admin4),
    # postalcodes — base columns
    Index("postalcodes_countrycode_idx",           t_postalcodes.c.countrycode),
    Index("postalcodes_admin1name_idx",            t_postalcodes.c.admin1name),
    Index("postalcodes_admin1code_idx",            t_postalcodes.c.admin1code),
    Index("postalcodes_admin2name_idx",            t_postalcodes.c.admin2name),
    Index("postalcodes_admin2code_idx",            t_postalcodes.c.admin2code),
    Index("postalcodes_admin3name_idx",            t_postalcodes.c.admin3name),
    Index("postalcodes_admin3code_idx",            t_postalcodes.c.admin3code),
    # postalcodes — enrichment columns
    Index("postalcodes_admin1code_full_idx",       t_postalcodes.c.admin1code_full),
    Index("postalcodes_admin2code_full_idx",       t_postalcodes.c.admin2code_full),
    Index("postalcodes_admin3code_full_idx",       t_postalcodes.c.admin3code_full),
    Index("postalcodes_admin1nameascii_idx",       t_postalcodes.c.admin1nameascii),
    Index("postalcodes_admin2nameascii_idx",       t_postalcodes.c.admin2nameascii),
    Index("postalcodes_admin3nameascii_idx",       t_postalcodes.c.admin3nameascii),
    # admin1codesascii
    Index("admin1codesascii_countrycode_idx",      t_admin1codesascii.c.countrycode),
    Index("admin1codesascii_name_idx",             t_admin1codesascii.c.name),
    Index("admin1codesascii_nameascii_idx",        t_admin1codesascii.c.nameascii),
    Index("admin1codesascii_code_idx",             t_admin1codesascii.c.code),
    # admin2codesascii
    Index("admin2codesascii_countrycode_idx",      t_admin2codesascii.c.countrycode),
    Index("admin2codesascii_name_idx",             t_admin2codesascii.c.name),
    Index("admin2codesascii_nameascii_idx",        t_admin2codesascii.c.nameascii),
    Index("admin2codesascii_code_idx",             t_admin2codesascii.c.code),
    # geoname + postalcodes — coordinate columns (B-tree)
    # Enable bounding-box pre-filtering on all dialects
    Index("geoname_latitude_idx",      t_geoname.c.latitude),
    Index("geoname_longitude_idx",     t_geoname.c.longitude),
    Index("postalcodes_latitude_idx",  t_postalcodes.c.latitude),
    Index("postalcodes_longitude_idx", t_postalcodes.c.longitude),
    # postalcodes — composite index for nearest-postal-code correlated subquery:
    # equality on countrycode + range on latitude allows the DB to scan only
    # postal codes in the right country within a lat/lon bounding box instead
    # of performing a full country scan for every geoname result row.
    Index("postalcodes_cc_lat_lon_idx",
          t_postalcodes.c.countrycode,
          t_postalcodes.c.latitude,
          t_postalcodes.c.longitude),
]