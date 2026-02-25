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
	  - PostgreSQL: uses earthdistance + GIST index (fast KNN via
	    earth_box). Requires load_geonames.py to have been run without
	    --skip-indexes.
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
	// geoRadiusM is the earth_box() pre-filter radius (PostgreSQL only).
	// Increase if the nearest result could be farther than this distance.
	geoRadiusM = 500_000 // 500 km
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
		countryClause = "  AND country = ?"
		args = []interface{}{lat, lon, lat, lon, geoRadiusM, country, limit}
	}
	rawSQL := fmt.Sprintf(`
		SELECT geonameid, name, fclass, fcode, country,
		       admin1, admin2, population, latitude, longitude,
		       earth_distance(
		           ll_to_earth(latitude, longitude),
		           ll_to_earth(?, ?)
		       ) / 1000.0 AS distance_km
		FROM geoname
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

// ---------------------------------------------------------------------------
// Haversine queries (MySQL / MariaDB / SQLite)
// ---------------------------------------------------------------------------

// haversineExpr returns a SQL distance expression (in km) for the fixed
// point (lat, lon) vs. the columns named "latitude" and "longitude".
// Uses repeated multiplication instead of POWER() for SQLite compatibility.
func haversineExpr(lat, lon float64) string {
	rad := math.Pi / 180.0
	cosLat := math.Cos(lat * rad)
	return fmt.Sprintf(
		`2.0 * %.10f * ASIN(SQRT(`+
			`SIN((latitude  - %.10f) * %.10f / 2.0)`+
			` * SIN((latitude  - %.10f) * %.10f / 2.0)`+
			` + %.10f * COS(latitude * %.10f)`+
			` * SIN((longitude - %.10f) * %.10f / 2.0)`+
			` * SIN((longitude - %.10f) * %.10f / 2.0)`+
			`))`,
		earthRadiusKm,
		lat, rad, lat, rad,
		cosLat, rad,
		lon, rad, lon, rad,
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
		countryClause = "  AND country = ?"
		args = []interface{}{country, limit}
	}
	rawSQL := fmt.Sprintf(`
		SELECT geonameid, name, fclass, fcode, country,
		       admin1, admin2, population, latitude, longitude,
		       %s AS distance_km
		FROM geoname
		WHERE latitude  IS NOT NULL
		  AND longitude IS NOT NULL
		%s
		ORDER BY distance_km
		LIMIT ?`, haversineExpr(lat, lon), countryClause)
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
		return queryPostalPostgres(db, lat, lon, limit, country)
	}
	return queryPostalHaversine(db, lat, lon, limit, country)
}

func queryGeoname(
	db *gorm.DB, lat, lon float64, limit int, country string,
) ([]GeonameResult, error) {
	if isPostgres(db) {
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
		strategy = "earthdistance (GIST index)"
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
