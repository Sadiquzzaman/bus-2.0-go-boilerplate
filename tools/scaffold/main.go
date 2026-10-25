// Command scaffold generates a new service from templates.
//
//	go run ./tools/scaffold -name booking -db postgres -kafka -redis
//	make new-service name=booking db=postgres kafka=true redis=true
//
// db: postgres | mongo | none. The generated service only contains code for
// what it uses; it imports the shared platform/ packages for everything else.
package main

import (
	"bufio"
	"bytes"
	"embed"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"text/template"
)

//go:embed all:templates
var templates embed.FS

type data struct {
	Name      string // service name, e.g. "booking"
	Module    string // Go module path of the repo
	DB        string // postgres | mongo | none
	HasDB     bool
	Kafka     bool
	Redis     bool
	Port      int
	AdminPort int
	DBName    string // database name (dashes → underscores)
}

var nameRE = regexp.MustCompile(`^[a-z][a-z0-9-]{1,40}[a-z0-9]$`)

func main() {
	var (
		name  = flag.String("name", "", "service name (lowercase, dashes allowed), e.g. booking")
		db    = flag.String("db", "postgres", "database: postgres | mongo | none")
		kafka = flag.Bool("kafka", true, "include Kafka producer (outbox) and consumer")
		redis = flag.Bool("redis", true, "include Redis (cache, idempotency keys)")
		port  = flag.Int("port", 0, "local HTTP port (default: next free slot from 8080)")
		root  = flag.String("root", ".", "repository root")
		force = flag.Bool("force", false, "overwrite an existing service directory")
	)
	flag.Parse()
	if err := run(*root, *name, *db, *kafka, *redis, *port, *force); err != nil {
		fmt.Fprintln(os.Stderr, "scaffold:", err)
		os.Exit(1)
	}
}

func run(root, name, db string, withKafka, withRedis bool, port int, force bool) error {
	if !nameRE.MatchString(name) || strings.Contains(name, "--") {
		return fmt.Errorf("invalid -name %q: use lowercase letters, digits and single dashes", name)
	}
	switch db {
	case "postgres", "mongo", "none":
	default:
		return fmt.Errorf("invalid -db %q: postgres | mongo | none", db)
	}
	module, err := readModule(filepath.Join(root, "go.mod"))
	if err != nil {
		return err
	}
	outDir := filepath.Join(root, "services", name)
	if _, err := os.Stat(outDir); err == nil && !force {
		return fmt.Errorf("%s already exists (use -force to overwrite)", outDir)
	}
	if port == 0 {
		port = nextPort(filepath.Join(root, "services"))
	}

	d := data{
		Name: name, Module: module, DB: db, HasDB: db != "none",
		Kafka: withKafka, Redis: withRedis,
		Port: port, AdminPort: port + 1,
		DBName: strings.ReplaceAll(name, "-", "_"),
	}

	var written []string
	err = fs.WalkDir(templates, "templates", func(path string, e fs.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		rel := strings.TrimPrefix(path, "templates/")
		if !include(rel, d) {
			return nil
		}
		src, err := templates.ReadFile(path)
		if err != nil {
			return err
		}
		tpl, err := template.New(rel).Delims("[[", "]]").Option("missingkey=error").Parse(string(src))
		if err != nil {
			return fmt.Errorf("parse %s: %w", rel, err)
		}
		var buf bytes.Buffer
		if err := tpl.Execute(&buf, d); err != nil {
			return fmt.Errorf("render %s: %w", rel, err)
		}
		out := buf.Bytes()
		dst := outputPath(rel, d)
		if strings.HasSuffix(dst, ".go") {
			formatted, err := format.Source(out)
			if err != nil {
				return fmt.Errorf("format %s: %w\n%s", dst, err, numbered(out))
			}
			out = formatted
		}
		full := filepath.Join(outDir, dst)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(full, out, 0o644); err != nil {
			return err
		}
		written = append(written, filepath.Join("services", name, dst))
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(written)
	fmt.Printf("created service %q (db=%s kafka=%v redis=%v, port %d)\n", name, db, withKafka, withRedis, port)
	for _, w := range written {
		fmt.Println("  " + w)
	}
	fmt.Println("\nnext steps:")
	if db == "postgres" {
		fmt.Printf("  make gen                                  # ent codegen\n")
		fmt.Printf("  make db svc=%s && make migrate-diff svc=%s name=init && make migrate-up svc=%s\n", name, name, name)
	}
	fmt.Printf("  cp services/%s/.env.example services/%s/.env\n", name, name)
	if withKafka {
		fmt.Printf("  make run svc=%s mode=topics\n", name)
	}
	fmt.Printf("  make run svc=%s mode=all\n", name)
	return nil
}

// include decides which template files apply to the chosen options.
func include(rel string, d data) bool {
	switch {
	case rel == "ent/schema/messaging.go.tmpl":
		return d.DB == "postgres" && d.Kafka
	case strings.HasPrefix(rel, "ent/"), rel == "atlas.hcl.tmpl", strings.HasPrefix(rel, "migrations/"):
		return d.DB == "postgres"
	case strings.HasPrefix(rel, "internal/store/postgres/"):
		return d.DB == "postgres"
	case strings.HasPrefix(rel, "internal/store/mongo/"):
		return d.DB == "mongo"
	case strings.HasPrefix(rel, "internal/domain/"), strings.HasPrefix(rel, "internal/service/"):
		return d.HasDB
	case strings.HasPrefix(rel, "internal/events/"), strings.HasPrefix(rel, "internal/consumer/"):
		return d.Kafka
	}
	return true
}

func outputPath(rel string, d data) string {
	p := strings.TrimSuffix(rel, ".tmpl")
	p = strings.ReplaceAll(p, "__NAME__", d.Name)
	switch filepath.Base(p) {
	case "env.example":
		p = filepath.Join(filepath.Dir(p), ".env.example")
	case "gitkeep":
		p = filepath.Join(filepath.Dir(p), ".gitkeep")
	}
	return p
}

func readModule(gomod string) (string, error) {
	f, err := os.Open(gomod)
	if err != nil {
		return "", fmt.Errorf("read go.mod (run from the repo root or pass -root): %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module ")), nil
		}
	}
	return "", errors.New("no module line in go.mod")
}

// nextPort gives each service its own local port pair: 8080/8081, 8090/8091 …
func nextPort(servicesDir string) int {
	entries, _ := os.ReadDir(servicesDir)
	n := 0
	for _, e := range entries {
		if e.IsDir() {
			n++
		}
	}
	return 8080 + 10*n
}

func numbered(b []byte) string {
	var sb strings.Builder
	for i, l := range strings.Split(string(b), "\n") {
		fmt.Fprintf(&sb, "%4d  %s\n", i+1, l)
	}
	return sb.String()
}
