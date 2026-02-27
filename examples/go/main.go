package main

/*
	reverse_geocode
	Package main provides a reverse geocoding example using GORM.
	Given a latitude and longitude, finds the nearest postal address
	and named place in the GeoNames database.

	Copyright (C) 2026 Rodolfo González González <code@rodolfo.gg>

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with this program.  If not, see <https://www.gnu.org/licenses/>.

	---------------------------------------------------------------------------

	Usage:
	    go run . --lat 19.4326 --lon -99.1332
	    go run . --lat 48.8566 --lon 2.3522 --results 5
	    go run . --lat 51.5074 --lon -0.1278 \
	        --url "postgres://user:pass@host/db"
	    go run . --lat 48.8566 --lon 2.3522 --country FR

	Build:
	    go build -o reverse_geocode .
	    ./reverse_geocode --lat 19.4326 --lon -99.1332

	Run "go mod tidy" once to resolve and download dependencies.

	Distance strategy (chosen automatically by dialect):
	  - PostgreSQL + Ganos (ganos_spatialref): uses ST_DWithin / ST_Distance
	    with a GIST index on ST_MakePoint(longitude, latitude)::geography
	    (preferred on Aliyun Apsara RDS for PostgreSQL).
	  - PostgreSQL + PostGIS: uses ST_DWithin / ST_Distance with a GIST
	    index on ST_MakePoint(longitude, latitude)::geography (preferred
	    when the PostGIS extension is installed).
	  - PostgreSQL (no Ganos/PostGIS): uses earthdistance + GIST index
	    (fast KNN via earth_box).
	  All PostgreSQL strategies require load_geonames.py to have been run
	  without --skip-indexes.
	  - MySQL/MariaDB, SQLite: Haversine formula executed entirely in SQL.
	    Falls back to a full table scan (no GIST equivalent).
	    SQLite requires CGO and math functions (SQLite >= 3.35).

	The --config flag points to the same YAML used by load_geonames.py.
	The --url flag accepts a connection URL and overrides --config:
	  postgresql+psycopg2://user:pass@host:5432/db  (Python compat)
	  postgres://user:pass@host:5432/db
	  mysql://user:pass@host:3306/db
	  sqlite:///path/to/file.db
*/

import (
	"flag"
	"fmt"
	"log"
	"math"
	"net/url"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const (
	earthRadiusKm = 6371.0
	// geoRadiusM is the earth_box() / ST_DWithin() pre-filter radius.
	// Increase if the nearest result could be farther than this distance.
	geoRadiusM = 500_000 // 500 km
	// degRadius is the approximate degree equivalent of geoRadiusM
	// (1° ≈ 111 320 m at the equator). Used as a bounding-box pre-filter on
	// lat/lon columns to let the DB use the composite B-tree index
	// (countrycode, latitude, longitude) before computing haversine ordering.
	degRadius = geoRadiusM / 111_320.0 // ≈ 4.5°
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

type dbConfig struct {
	URL      string `yaml:"url"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Dbname   string `yaml:"dbname"`
}

// Config mirrors the structure of the geonames-loader config YAML.
type Config struct {
	Database dbConfig `yaml:"database"`
}

func loadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening config %q: %w", path, err)
	}
	defer f.Close()

	var cfg Config
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parsing config %q: %w", path, err)
	}
	return &cfg, nil
}

// ---------------------------------------------------------------------------
// Database connection
// ---------------------------------------------------------------------------

// mysqlURLtoDSN converts mysql://user:pass@host:port/dbname to GORM format.
func mysqlURLtoDSN(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("invalid MySQL URL: %w", err)
	}
	user, pass := "", ""
	if u.User != nil {
		user = u.User.Username()
		pass, _ = u.User.Password()
	}
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":3306"
	}
	return fmt.Sprintf(
		"%s:%s@tcp(%s)%s?charset=utf8mb4&parseTime=True&loc=Local",
		user, pass, host, u.Path,
	), nil
}

// openDB returns a *gorm.DB from --url or the legacy YAML fields.
func openDB(cfg *Config, rawURL string) (*gorm.DB, error) {
	gCfg := &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	}

	dsn := rawURL
	if dsn == "" {
		dsn = cfg.Database.URL
	}

	if dsn != "" {
		// Normalise Python SQLAlchemy prefixes to GORM-compatible ones.
		dsn = strings.ReplaceAll(dsn, "postgresql+psycopg2://", "postgres://")
		dsn = strings.ReplaceAll(dsn, "postgresql://", "postgres://")

		switch {
		case strings.HasPrefix(dsn, "postgres://"):
			return gorm.Open(postgres.Open(dsn), gCfg)
		case strings.HasPrefix(dsn, "mysql://"):
			mDSN, err := mysqlURLtoDSN(dsn)
			if err != nil {
				return nil, err
			}
			return gorm.Open(mysql.Open(mDSN), gCfg)
		case strings.HasPrefix(dsn, "sqlite://"):
			// sqlite:///path/to/file  →  /path/to/file
			path := strings.TrimPrefix(dsn, "sqlite://")
			return gorm.Open(sqlite.Open(path), gCfg)
		default:
			// Treat as a raw PostgreSQL DSN (host=... user=... ...)
			return gorm.Open(postgres.Open(dsn), gCfg)
		}
	}

	// Fall back to legacy YAML fields → build PostgreSQL DSN.
	port := cfg.Database.Port
	if port == 0 {
		port = 5432
	}
	legacyDSN := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.Database.Host, port,
		cfg.Database.User, cfg.Database.Password, cfg.Database.Dbname,
	)
	return gorm.Open(postgres.Open(legacyDSN), gCfg)
}

func isPostgres(db *gorm.DB) bool {
	return db.Dialector.Name() == "postgres"
}

func hasPostGIS(db *gorm.DB) bool {
	var count int64
	db.Raw("SELECT count(*) FROM pg_extension WHERE extname = 'postgis'").Scan(&count)
	return count > 0
}

// hasGanos returns true if the ganos_spatialref extension is installed.
func hasGanos(db *gorm.DB) bool {
	var count int64
	db.Raw("SELECT count(*) FROM pg_extension WHERE extname = 'ganos_spatialref'").Scan(&count)
	return count > 0
}

// hasGeographyType returns true if the 'geography' PostgreSQL type is actually
// registered in pg_type.
//
// Checking for the extension alone (ganos_spatialref or postgis) is not
// sufficient: on some Aliyun Apsara RDS configurations ganos_spatialref is
// present but the geography type is absent because ganos_geometry was not
// installed with CASCADE.  The ::geography cast — used in all ST_DWithin /
// ST_Distance queries and indexes — raises a SyntaxError if the type is
// missing.  This function is the real gate for the geography-based strategy.
func hasGeographyType(db *gorm.DB) bool {
	var count int64
	db.Raw("SELECT count(*) FROM pg_type WHERE typname = 'geography'").Scan(&count)
	return count > 0
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

// PostalResult holds one row from the postalcodes proximity query.
type PostalResult struct {
	Countrycode string  `gorm:"column:countrycode"`
	Postalcode  string  `gorm:"column:postalcode"`
	Placename   string  `gorm:"column:placename"`
	Admin1name  string  `gorm:"column:admin1name"`
	Admin2name  string  `gorm:"column:admin2name"`
	Admin3name  string  `gorm:"column:admin3name"`
	Latitude    float64 `gorm:"column:latitude"`
	Longitude   float64 `gorm:"column:longitude"`
	DistanceKm  float64 `gorm:"column:distance_km"`
}

// GeonameResult holds one row from the geoname proximity query.
type GeonameResult struct {
	Geonameid  int64   `gorm:"column:geonameid"`
	Name       string  `gorm:"column:name"`
	Fclass     string  `gorm:"column:fclass"`
	Fcode      string  `gorm:"column:fcode"`
	Country    string  `gorm:"column:country"`
	Admin1     string  `gorm:"column:admin1"`
	Admin2     string  `gorm:"column:admin2"`
	Population int64   `gorm:"column:population"`
	Latitude   float64 `gorm:"column:latitude"`
	Longitude  float64 `gorm:"column:longitude"`
	DistanceKm float64 `gorm:"column:distance_km"`
	Postalcode string  `gorm:"column:postalcode"`
}

// ---------------------------------------------------------------------------
// PostgreSQL PostGIS queries (use GIST index via ST_DWithin)
// ---------------------------------------------------------------------------

func queryPostalPostGIS(
	db *gorm.DB, lat, lon float64, limit int, country string,
) ([]PostalResult, error) {
	var rows []PostalResult
	countryClause := ""
	args := []interface{}{lon, lat, lon, lat, geoRadiusM, limit}
	if country != "" {
		countryClause = "  AND countrycode = ?"
		args = []interface{}{lon, lat, lon, lat, geoRadiusM, country, limit}
	}
	rawSQL := fmt.Sprintf(`
		SELECT countrycode, postalcode, placename,
		       admin1name, admin2name, admin3name,
		       latitude, longitude,
		       ST_Distance(
		           ST_MakePoint(longitude, latitude)::geography,
		           ST_MakePoint(?, ?)::geography
		       ) / 1000.0 AS distance_km
		FROM postalcodes
		WHERE latitude  IS NOT NULL
		  AND longitude IS NOT NULL
		  AND ST_DWithin(
		          ST_MakePoint(longitude, latitude)::geography,
		          ST_MakePoint(?, ?)::geography,
		          ?
		      )
		%s
		ORDER BY distance_km
		LIMIT ?`, countryClause)
	res := db.Raw(rawSQL, args...).Scan(&rows)
	return rows, res.Error
}

func queryGeonamePostGIS(
	db *gorm.DB, lat, lon float64, limit int, country string,
) ([]GeonameResult, error) {
	var rows []GeonameResult
	countryClause := ""
	args := []interface{}{lon, lat, lon, lat, geoRadiusM, limit}
	if country != "" {
		countryClause = "  AND g.country = ?"
		args = []interface{}{lon, lat, lon, lat, geoRadiusM, country, limit}
	}
	rawSQL := fmt.Sprintf(`
		SELECT g.geonameid, g.name, g.fclass, g.fcode, g.country,
		       g.admin1, g.admin2, g.population, g.latitude, g.longitude,
		       ST_Distance(
		           ST_MakePoint(g.longitude, g.latitude)::geography,
		           ST_MakePoint(?, ?)::geography
		       ) / 1000.0 AS distance_km,
		       pc.postalcode
		FROM geoname g
		LEFT JOIN LATERAL (
		    SELECT postalcode FROM postalcodes
		    WHERE countrycode = g.country
		      AND latitude  IS NOT NULL AND longitude IS NOT NULL
		      AND latitude  BETWEEN g.latitude  - %.4f AND g.latitude  + %.4f
		      AND longitude BETWEEN g.longitude - %.4f AND g.longitude + %.4f
		    ORDER BY ST_MakePoint(longitude, latitude)::geography
		             <-> ST_MakePoint(g.longitude, g.latitude)::geography
		    LIMIT 1
		) pc ON true
		WHERE g.latitude  IS NOT NULL
		  AND g.longitude IS NOT NULL
		  AND ST_DWithin(
		          ST_MakePoint(g.longitude, g.latitude)::geography,
		          ST_MakePoint(?, ?)::geography,
		          ?
		      )
		%s
		ORDER BY distance_km
		LIMIT ?`, degRadius, degRadius, degRadius, degRadius, countryClause)
	res := db.Raw(rawSQL, args...).Scan(&rows)
	return rows, res.Error
}

// ---------------------------------------------------------------------------
// PostgreSQL earthdistance queries (use GIST index via earth_box)
// ---------------------------------------------------------------------------

func queryPostalPostgres(
	db *gorm.DB, lat, lon float64, limit int, country string,
) ([]PostalResult, error) {
	var rows []PostalResult
	countryClause := ""
	args := []interface{}{lat, lon, lat, lon, geoRadiusM, limit}
	if country != "" {
		countryClause = "  AND countrycode = ?"
		args = []interface{}{lat, lon, lat, lon, geoRadiusM, country, limit}
	}
	rawSQL := fmt.Sprintf(`
		SELECT countrycode, postalcode, placename,
		       admin1name, admin2name, admin3name,
		       latitude, longitude,
		       earth_distance(
		           ll_to_earth(latitude, longitude),
		           ll_to_earth(?, ?)
		       ) / 1000.0 AS distance_km
		FROM postalcodes
		WHERE latitude  IS NOT NULL
		  AND longitude IS NOT NULL
		  AND earth_box(ll_to_earth(?, ?), ?)
		      @> ll_to_earth(latitude, longitude)
		%s
		ORDER BY distance_km
		LIMIT ?`, countryClause)
	res := db.Raw(rawSQL, args...).Scan(&rows)
	return rows, res.Error
}

func queryGeonamePostgres(
	db *gorm.DB, lat, lon float64, limit int, country string,
) ([]GeonameResult, error) {
	var rows []GeonameResult
	countryClause := ""
	args := []interface{}{lat, lon, lat, lon, geoRadiusM, limit}
	if country != "" {
		countryClause = "  AND g.country = ?"
		args = []interface{}{lat, lon, lat, lon, geoRadiusM, country, limit}
	}
	rawSQL := fmt.Sprintf(`
		SELECT g.geonameid, g.name, g.fclass, g.fcode, g.country,
		       g.admin1, g.admin2, g.population, g.latitude, g.longitude,
		       earth_distance(
		           ll_to_earth(g.latitude, g.longitude),
		           ll_to_earth(?, ?)
		       ) / 1000.0 AS distance_km,
		       pc.postalcode
		FROM geoname g
		LEFT JOIN LATERAL (
		    SELECT postalcode FROM postalcodes
		    WHERE countrycode = g.country
		      AND latitude  IS NOT NULL AND longitude IS NOT NULL
		      AND latitude  BETWEEN g.latitude  - %.4f AND g.latitude  + %.4f
		      AND longitude BETWEEN g.longitude - %.4f AND g.longitude + %.4f
		    ORDER BY ll_to_earth(latitude, longitude)
		             <-> ll_to_earth(g.latitude, g.longitude)
		    LIMIT 1
		) pc ON true
		WHERE g.latitude  IS NOT NULL
		  AND g.longitude IS NOT NULL
		  AND earth_box(ll_to_earth(?, ?), ?)
		      @> ll_to_earth(g.latitude, g.longitude)
		%s
		ORDER BY distance_km
		LIMIT ?`, degRadius, degRadius, degRadius, degRadius, countryClause)
	res := db.Raw(rawSQL, args...).Scan(&rows)
	return rows, res.Error
}

// ---------------------------------------------------------------------------
// Haversine queries (MySQL / MariaDB / SQLite)
// ---------------------------------------------------------------------------

// haversineExpr returns a SQL distance expression (in km) for the fixed
// point (lat, lon) vs. the columns named "latitude" and "longitude".
// Uses repeated multiplication instead of POWER() for SQLite compatibility.
func haversineExpr(lat, lon float64) string {
	return haversineExprAlias(lat, lon, "")
}

// haversineExprAlias is like haversineExpr but prefixes column names with
// the given table alias (e.g. "g" → "g.latitude"). Pass "" for no alias.
func haversineExprAlias(lat, lon float64, alias string) string {
	rad := math.Pi / 180.0
	cosLat := math.Cos(lat * rad)
	latCol, lonCol := "latitude", "longitude"
	if alias != "" {
		latCol = alias + ".latitude"
		lonCol = alias + ".longitude"
	}
	return fmt.Sprintf(
		`2.0 * %.10f * ASIN(SQRT(`+
			`SIN((%s - %.10f) * %.10f / 2.0)`+
			` * SIN((%s - %.10f) * %.10f / 2.0)`+
			` + %.10f * COS(%s * %.10f)`+
			` * SIN((%s - %.10f) * %.10f / 2.0)`+
			` * SIN((%s - %.10f) * %.10f / 2.0)`+
			`))`,
		earthRadiusKm,
		latCol, lat, rad, latCol, lat, rad,
		cosLat, latCol, rad,
		lonCol, lon, rad, lonCol, lon, rad,
	)
}

// haversineColExpr returns a SQL expression for the Haversine distance (km)
// between two column-referenced points using table aliases "g" (geoname) and
// "p" (postalcodes). Used in correlated subqueries for nearest postal code.
func haversineColExpr() string {
	rad := math.Pi / 180.0
	return fmt.Sprintf(
		`2.0 * %.10f * ASIN(SQRT(`+
			`SIN((p.latitude  - g.latitude)  * %.10f / 2.0)`+
			` * SIN((p.latitude  - g.latitude)  * %.10f / 2.0)`+
			` + COS(g.latitude * %.10f) * COS(p.latitude * %.10f)`+
			` * SIN((p.longitude - g.longitude) * %.10f / 2.0)`+
			` * SIN((p.longitude - g.longitude) * %.10f / 2.0)`+
			`))`,
		earthRadiusKm,
		rad, rad,
		rad, rad,
		rad, rad,
	)
}

func queryPostalHaversine(
	db *gorm.DB, lat, lon float64, limit int, country string,
) ([]PostalResult, error) {
	var rows []PostalResult
	countryClause := ""
	args := []interface{}{limit}
	if country != "" {
		countryClause = "  AND countrycode = ?"
		args = []interface{}{country, limit}
	}
	rawSQL := fmt.Sprintf(`
		SELECT countrycode, postalcode, placename,
		       admin1name, admin2name, admin3name,
		       latitude, longitude,
		       %s AS distance_km
		FROM postalcodes
		WHERE latitude  IS NOT NULL
		  AND longitude IS NOT NULL
		%s
		ORDER BY distance_km
		LIMIT ?`, haversineExpr(lat, lon), countryClause)
	res := db.Raw(rawSQL, args...).Scan(&rows)
	return rows, res.Error
}

func queryGeonameHaversine(
	db *gorm.DB, lat, lon float64, limit int, country string,
) ([]GeonameResult, error) {
	var rows []GeonameResult
	countryClause := ""
	args := []interface{}{limit}
	if country != "" {
		countryClause = "  AND g.country = ?"
		args = []interface{}{country, limit}
	}
	rawSQL := fmt.Sprintf(`
		SELECT g.geonameid, g.name, g.fclass, g.fcode, g.country,
		       g.admin1, g.admin2, g.population, g.latitude, g.longitude,
		       %s AS distance_km,
		       (SELECT p.postalcode FROM postalcodes p
		        WHERE p.countrycode = g.country
		          AND p.latitude  IS NOT NULL AND p.longitude IS NOT NULL
		          AND p.latitude  BETWEEN g.latitude  - %.4f AND g.latitude  + %.4f
		          AND p.longitude BETWEEN g.longitude - %.4f AND g.longitude + %.4f
		        ORDER BY %s
		        LIMIT 1) AS postalcode
		FROM geoname g
		WHERE g.latitude  IS NOT NULL
		  AND g.longitude IS NOT NULL
		%s
		ORDER BY distance_km
		LIMIT ?`,
		haversineExprAlias(lat, lon, "g"),
		degRadius, degRadius, degRadius, degRadius,
		haversineColExpr(),
		countryClause)
	res := db.Raw(rawSQL, args...).Scan(&rows)
	return rows, res.Error
}

// ---------------------------------------------------------------------------
// Query dispatchers
// ---------------------------------------------------------------------------

func queryPostal(
	db *gorm.DB, lat, lon float64, limit int, country string,
) ([]PostalResult, error) {
	if isPostgres(db) {
		if hasGeographyType(db) {
			return queryPostalPostGIS(db, lat, lon, limit, country)
		}
		return queryPostalPostgres(db, lat, lon, limit, country)
	}
	return queryPostalHaversine(db, lat, lon, limit, country)
}

func queryGeoname(
	db *gorm.DB, lat, lon float64, limit int, country string,
) ([]GeonameResult, error) {
	if isPostgres(db) {
		if hasGeographyType(db) {
			return queryGeonamePostGIS(db, lat, lon, limit, country)
		}
		return queryGeonamePostgres(db, lat, lon, limit, country)
	}
	return queryGeonameHaversine(db, lat, lon, limit, country)
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

func printPostal(rows []PostalResult) {
	fmt.Printf("Nearest postal-code entries (%d result(s)):\n\n", len(rows))
	for _, r := range rows {
		fmt.Printf("  Country     : %s\n", r.Countrycode)
		fmt.Printf("  Postal code : %s\n", r.Postalcode)
		fmt.Printf("  Place       : %s\n", r.Placename)
		if r.Admin3name != "" {
			fmt.Printf("  Admin 3     : %s\n", r.Admin3name)
		}
		if r.Admin2name != "" {
			fmt.Printf("  Admin 2     : %s\n", r.Admin2name)
		}
		if r.Admin1name != "" {
			fmt.Printf("  Admin 1     : %s\n", r.Admin1name)
		}
		fmt.Printf("  Coordinates : %g, %g\n", r.Latitude, r.Longitude)
		fmt.Printf("  Distance    : %.3f km\n\n", r.DistanceKm)
	}
}

func printGeoname(rows []GeonameResult) {
	fmt.Printf("Nearest geoname entries (%d result(s)):\n\n", len(rows))
	for _, r := range rows {
		fmt.Printf("  GeoName ID  : %d\n", r.Geonameid)
		fmt.Printf("  Name        : %s\n", r.Name)
		fmt.Printf("  Country     : %s\n", r.Country)
		fmt.Printf("  Feature     : %s/%s\n", r.Fclass, r.Fcode)
		fmt.Printf("  Population  : %d\n", r.Population)
		if r.Postalcode != "" {
			fmt.Printf("  Postal code : %s\n", r.Postalcode)
		}
		fmt.Printf("  Coordinates : %g, %g\n", r.Latitude, r.Longitude)
		fmt.Printf("  Distance    : %.3f km\n\n", r.DistanceKm)
	}
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	lat := flag.Float64(
		"lat", math.NaN(),
		"Latitude in decimal degrees (required, e.g. 19.4326)",
	)
	lon := flag.Float64(
		"lon", math.NaN(),
		"Longitude in decimal degrees (required, e.g. -99.1332)",
	)
	cfgPath := flag.String(
		"config", "../../config/config.yaml",
		"Path to config YAML file (default: ../../config/config.yaml)",
	)
	rawURL := flag.String(
		"url", "",
		"Connection URL — overrides --config "+
			"(e.g. postgres://user:pass@host/db)",
	)
	nRes := flag.Int(
		"results", 3,
		"Number of nearest results to return (default: 3)",
	)
	country := flag.String(
		"country", "",
		"Restrict results to this ISO 3166-1 alpha-2 country code "+
			"(e.g. MX, FR, DE). If omitted, all countries are searched.",
	)
	flag.Parse()

	if math.IsNaN(*lat) || math.IsNaN(*lon) {
		fmt.Fprintln(os.Stderr, "ERROR: --lat and --lon are required.")
		flag.Usage()
		os.Exit(1)
	}
	if *lat < -90 || *lat > 90 {
		fmt.Fprintln(os.Stderr, "ERROR: --lat must be between -90 and 90.")
		os.Exit(1)
	}
	if *lon < -180 || *lon > 180 {
		fmt.Fprintln(os.Stderr,
			"ERROR: --lon must be between -180 and 180.")
		os.Exit(1)
	}

	var cfg *Config
	if *rawURL == "" {
		var err error
		cfg, err = loadConfig(*cfgPath)
		if err != nil {
			log.Fatalf("config: %v", err)
		}
	} else {
		cfg = new(Config)
	}

	db, err := openDB(cfg, *rawURL)
	if err != nil {
		log.Fatalf("database: %v", err)
	}

	strategy := "Haversine (full scan)"
	if isPostgres(db) {
		if hasGeographyType(db) {
			if hasGanos(db) {
				strategy = "Ganos/ganos_spatialref (GIST index)"
			} else {
				strategy = "PostGIS (GIST index)"
			}
		} else {
			strategy = "earthdistance (GIST index)"
		}
	}

	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("GeoNames reverse geocoder — Go / GORM")
	fmt.Printf("  Latitude  : %g\n", *lat)
	fmt.Printf("  Longitude : %g\n", *lon)
	fmt.Printf("  Results   : %d\n", *nRes)
	if *country != "" {
		fmt.Printf("  Country   : %s\n", *country)
	}
	fmt.Printf("  Strategy  : %s\n", strategy)
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println()

	postalRows, err := queryPostal(db, *lat, *lon, *nRes, *country)
	if err != nil {
		log.Fatalf("postal query: %v", err)
	}
	if len(postalRows) > 0 {
		printPostal(postalRows)
	} else {
		fmt.Println("No postal-code data found for these coordinates.")
	}

	fmt.Println(strings.Repeat("-", 60))
	fmt.Println()

	geoRows, err := queryGeoname(db, *lat, *lon, *nRes, *country)
	if err != nil {
		log.Fatalf("geoname query: %v", err)
	}
	if len(geoRows) > 0 {
		printGeoname(geoRows)
	} else {
		fmt.Println("No geoname entries found.")
	}
}
