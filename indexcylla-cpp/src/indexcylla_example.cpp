#include <cassandra.h>
#include <iostream>
#include <string>
#include <vector>

/**
 * Ejemplo básico de indexador ScyllaDB
 * Muestra los conceptos fundamentales sin complejidad adicional
 */

class SimpleScyllaIndexer {
private:
    CassCluster* cluster;
    CassSession* session;
    const CassPrepared* prepared_stmt;

public:
    SimpleScyllaIndexer(const std::string& hosts) {
        // Configurar cluster
        cluster = cass_cluster_new();
        session = cass_session_new();

        cass_cluster_set_contact_points(cluster, hosts.c_str());
        cass_cluster_set_protocol_version(cluster, CASS_PROTOCOL_VERSION_V4);

        // Conectar
        CassFuture* connect_future = cass_session_connect(session, cluster);
        CassError rc = cass_future_error_code(connect_future);
        if (rc != CASS_OK) {
            throw std::runtime_error("Error conectando: " + std::string(cass_error_desc(rc)));
        }
        cass_future_free(connect_future);

        // Crear schema
        setup_schema();

        // Preparar statement
        prepare_statements();
    }

    ~SimpleScyllaIndexer() {
        cleanup();
    }

    // Indexar un documento simple
    bool index_document(const std::string& id, const std::string& content) {
        CassStatement* statement = cass_prepared_bind(prepared_stmt);

        cass_statement_bind_string(statement, 0, id.c_str());
        cass_statement_bind_string(statement, 1, content.c_str());

        CassFuture* result_future = cass_session_execute(session, statement);
        CassError rc = cass_future_error_code(result_future);

        bool success = (rc == CASS_OK);
        if (!success) {
            std::cerr << "Error: " << cass_error_desc(rc) << std::endl;
        }

        cass_statement_free(statement);
        cass_future_free(result_future);

        return success;
    }

    // Indexar múltiples documentos usando batch
    bool index_batch(const std::vector<std::pair<std::string, std::string>>& docs) {
        CassBatch* batch = cass_batch_new(CASS_BATCH_TYPE_UNLOGGED);

        for (const auto& doc : docs) {
            CassStatement* statement = cass_prepared_bind(prepared_stmt);
            cass_statement_bind_string(statement, 0, doc.first.c_str());  // id
            cass_statement_bind_string(statement, 1, doc.second.c_str()); // content

            cass_batch_add_statement(batch, statement);
            cass_statement_free(statement);
        }

        CassFuture* batch_future = cass_session_execute_batch(session, batch);
        CassError rc = cass_future_error_code(batch_future);

        bool success = (rc == CASS_OK);
        if (!success) {
            std::cerr << "Batch error: " << cass_error_desc(rc) << std::endl;
        }

        cass_batch_free(batch);
        cass_future_free(batch_future);

        return success;
    }

private:
    void setup_schema() {
        execute_query("CREATE KEYSPACE IF NOT EXISTS simple_indexer "
                     "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}");

        execute_query("CREATE TABLE IF NOT EXISTS simple_indexer.documents ("
                     "id text PRIMARY KEY, "
                     "content text, "
                     "created_at timestamp DEFAULT toTimestamp(now()))");

        execute_query("CREATE INDEX IF NOT EXISTS ON simple_indexer.documents (created_at)");
    }

    void prepare_statements() {
        const char* query = "INSERT INTO simple_indexer.documents (id, content) VALUES (?, ?)";

        CassFuture* prepare_future = cass_session_prepare(session, query);
        CassError rc = cass_future_error_code(prepare_future);
        if (rc != CASS_OK) {
            cass_future_free(prepare_future);
            throw std::runtime_error("Error preparando statement");
        }

        prepared_stmt = cass_future_get_prepared(prepare_future);
        cass_future_free(prepare_future);
    }

    void execute_query(const char* query) {
        CassStatement* statement = cass_statement_new(query, 0);
        CassFuture* result_future = cass_session_execute(session, statement);
        cass_future_wait(result_future);
        cass_statement_free(statement);
        cass_future_free(result_future);
    }

    void cleanup() {
        if (prepared_stmt) cass_prepared_free(prepared_stmt);
        if (session) {
            CassFuture* close_future = cass_session_close(session);
            cass_future_wait(close_future);
            cass_future_free(close_future);
            cass_session_free(session);
        }
        if (cluster) cass_cluster_free(cluster);
    }
};

// Ejemplo de uso básico
int main() {
    try {
        SimpleScyllaIndexer indexer("127.0.0.1");

        // Indexar documento individual
        std::cout << "Indexando documento individual..." << std::endl;
        bool success = indexer.index_document("doc1", "Contenido del primer documento");
        std::cout << "Resultado: " << (success ? "Éxito" : "Error") << std::endl;

        // Indexar batch de documentos
        std::cout << "\nIndexando batch de documentos..." << std::endl;
        std::vector<std::pair<std::string, std::string>> docs = {
            {"doc2", "Segundo documento"},
            {"doc3", "Tercer documento"},
            {"doc4", "Cuarto documento"}
        };

        success = indexer.index_batch(docs);
        std::cout << "Batch resultado: " << (success ? "Éxito" : "Error") << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}