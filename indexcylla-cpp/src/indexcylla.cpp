#include <cassandra.h>
#include <iostream>
#include <vector>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <future>
#include <memory>
#include <string>
#include <chrono>

/**
 * ScyllaDB Indexer - Indexador masivo de datos en C++
 * Implementa patrones Producer-Consumer y Thread Pool para
 * procesar grandes volúmenes de datos de manera eficiente
 */

class ScyllaDBIndexer {
private:
    // Configuración de la conexión
    CassCluster* cluster;
    CassSession* session;
    const CassPrepared* prepared_insert;
    const CassPrepared* prepared_update;

    // Thread Pool para procesamiento paralelo
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> task_queue;
    std::mutex queue_mutex;
    std::condition_variable condition;
    std::atomic<bool> stop_flag{false};

    // Configuración del indexador
    size_t max_batch_size;
    size_t thread_pool_size;
    std::atomic<size_t> processed_count{0};
    std::atomic<size_t> error_count{0};

public:
    struct IndexDocument {
        std::string id;
        std::string content;
        std::string metadata;
        int64_t timestamp;

        IndexDocument(const std::string& id, const std::string& content, 
                     const std::string& metadata)
            : id(id), content(content), metadata(metadata),
              timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::system_clock::now().time_since_epoch()).count()) {}
    };

    ScyllaDBIndexer(const std::string& hosts, size_t pool_size = 8, 
                   size_t batch_size = 1000) 
        : max_batch_size(batch_size), thread_pool_size(pool_size) {

        // Inicializar cluster y sesión
        cluster = cass_cluster_new();
        session = cass_session_new();

        // Configurar conexión optimizada para alta carga
        cass_cluster_set_contact_points(cluster, hosts.c_str());
        cass_cluster_set_protocol_version(cluster, CASS_PROTOCOL_VERSION_V4);
        cass_cluster_set_num_threads_io(cluster, 4);
        cass_cluster_set_queue_size_io(cluster, 8192);
        cass_cluster_set_core_connections_per_host(cluster, 2);
        cass_cluster_set_max_connections_per_host(cluster, 4);
        cass_cluster_set_max_requests_per_flush(cluster, 128);
        cass_cluster_set_write_bytes_high_water_mark(cluster, 32 * 1024);
        cass_cluster_set_write_bytes_low_water_mark(cluster, 16 * 1024);

        // Conectar a ScyllaDB
        CassFuture* connect_future = cass_session_connect(session, cluster);
        CassError rc = cass_future_error_code(connect_future);
        if (rc != CASS_OK) {
            std::cerr << "Error conectando a ScyllaDB: " << cass_error_desc(rc) << std::endl;
            cass_future_free(connect_future);
            throw std::runtime_error("No se pudo conectar a ScyllaDB");
        }
        cass_future_free(connect_future);

        // Preparar statements para mejor rendimiento
        prepare_statements();

        // Inicializar thread pool
        initialize_thread_pool();
    }

    ~ScyllaDBIndexer() {
        shutdown();
        cleanup();
    }

private:
    void prepare_statements() {
        // Crear keyspace y tabla si no existen
        const char* create_keyspace = 
            "CREATE KEYSPACE IF NOT EXISTS indexer_ks "
            "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3}";

        const char* create_table = 
            "CREATE TABLE IF NOT EXISTS indexer_ks.documents ("
            "id text PRIMARY KEY, "
            "content text, "
            "metadata text, "
            "timestamp bigint, "
            "indexed_at timestamp"
            ")";

        const char* create_index = 
            "CREATE INDEX IF NOT EXISTS ON indexer_ks.documents (timestamp)";

        execute_simple_statement(create_keyspace);
        execute_simple_statement(create_table);
        execute_simple_statement(create_index);

        // Preparar statements para inserción y actualización
        const char* insert_query = 
            "INSERT INTO indexer_ks.documents (id, content, metadata, timestamp, indexed_at) "
            "VALUES (?, ?, ?, ?, toTimestamp(now()))";

        const char* update_query = 
            "UPDATE indexer_ks.documents SET content = ?, metadata = ?, "
            "timestamp = ?, indexed_at = toTimestamp(now()) WHERE id = ?";

        CassFuture* prep_future = cass_session_prepare(session, insert_query);
        CassError rc = cass_future_error_code(prep_future);
        if (rc != CASS_OK) {
            std::cerr << "Error preparando INSERT: " << cass_error_desc(rc) << std::endl;
            cass_future_free(prep_future);
            throw std::runtime_error("Error preparando statements");
        }
        prepared_insert = cass_future_get_prepared(prep_future);
        cass_future_free(prep_future);

        prep_future = cass_session_prepare(session, update_query);
        rc = cass_future_error_code(prep_future);
        if (rc != CASS_OK) {
            std::cerr << "Error preparando UPDATE: " << cass_error_desc(rc) << std::endl;
            cass_future_free(prep_future);
            throw std::runtime_error("Error preparando statements");
        }
        prepared_update = cass_future_get_prepared(prep_future);
        cass_future_free(prep_future);
    }

    void execute_simple_statement(const char* query) {
        CassStatement* statement = cass_statement_new(query, 0);
        CassFuture* query_future = cass_session_execute(session, statement);
        CassError rc = cass_future_error_code(query_future);
        if (rc != CASS_OK) {
            std::cerr << "Error ejecutando: " << query << " - " << cass_error_desc(rc) << std::endl;
        }
        cass_statement_free(statement);
        cass_future_free(query_future);
    }

    void initialize_thread_pool() {
        for (size_t i = 0; i < thread_pool_size; ++i) {
            workers.emplace_back([this] {
                worker_thread();
            });
        }
    }

    void worker_thread() {
        while (true) {
            std::function<void()> task;

            {
                std::unique_lock<std::mutex> lock(queue_mutex);
                condition.wait(lock, [this] { return stop_flag || !task_queue.empty(); });

                if (stop_flag && task_queue.empty()) {
                    break;
                }

                task = std::move(task_queue.front());
                task_queue.pop();
            }

            try {
                task();
            } catch (const std::exception& e) {
                std::cerr << "Error en worker thread: " << e.what() << std::endl;
                error_count++;
            }
        }
    }

public:
    // Método principal para indexar un documento individual
    void index_document(const IndexDocument& doc) {
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            task_queue.emplace([this, doc] {
                process_document(doc);
            });
        }
        condition.notify_one();
    }

    // Método para indexar múltiples documentos usando batch processing
    void index_documents_batch(const std::vector<IndexDocument>& documents) {
        if (documents.empty()) return;

        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            task_queue.emplace([this, documents] {
                process_documents_batch(documents);
            });
        }
        condition.notify_one();
    }

    // Método para indexar en chunks usando UNLOGGED batch para máximo rendimiento
    void index_documents_parallel(const std::vector<IndexDocument>& documents) {
        if (documents.empty()) return;

        // Dividir en chunks para procesamiento paralelo
        size_t chunk_size = std::max(size_t(1), documents.size() / thread_pool_size);
        std::vector<std::future<void>> futures;

        for (size_t i = 0; i < documents.size(); i += chunk_size) {
            size_t end = std::min(i + chunk_size, documents.size());
            std::vector<IndexDocument> chunk(documents.begin() + i, documents.begin() + end);

            std::promise<void> promise;
            futures.push_back(promise.get_future());

            {
                std::lock_guard<std::mutex> lock(queue_mutex);
                task_queue.emplace([this, chunk, promise = std::move(promise)]() mutable {
                    try {
                        process_documents_batch(chunk);
                        promise.set_value();
                    } catch (...) {
                        promise.set_exception(std::current_exception());
                    }
                });
            }
            condition.notify_one();
        }

        // Esperar a que todos los chunks se procesen
        for (auto& future : futures) {
            try {
                future.get();
            } catch (const std::exception& e) {
                std::cerr << "Error procesando chunk: " << e.what() << std::endl;
                error_count++;
            }
        }
    }

private:
    void process_document(const IndexDocument& doc) {
        CassStatement* statement = cass_prepared_bind(prepared_insert);

        cass_statement_bind_string(statement, 0, doc.id.c_str());
        cass_statement_bind_string(statement, 1, doc.content.c_str());
        cass_statement_bind_string(statement, 2, doc.metadata.c_str());
        cass_statement_bind_int64(statement, 3, doc.timestamp);

        CassFuture* query_future = cass_session_execute(session, statement);
        CassError rc = cass_future_error_code(query_future);

        if (rc != CASS_OK) {
            std::cerr << "Error indexando documento " << doc.id 
                      << ": " << cass_error_desc(rc) << std::endl;
            error_count++;
        } else {
            processed_count++;
        }

        cass_statement_free(statement);
        cass_future_free(query_future);
    }

    void process_documents_batch(const std::vector<IndexDocument>& documents) {
        // Usar UNLOGGED batch para máximo rendimiento cuando no se requiere atomicidad
        CassBatch* batch = cass_batch_new(CASS_BATCH_TYPE_UNLOGGED);

        for (const auto& doc : documents) {
            CassStatement* statement = cass_prepared_bind(prepared_insert);

            cass_statement_bind_string(statement, 0, doc.id.c_str());
            cass_statement_bind_string(statement, 1, doc.content.c_str());
            cass_statement_bind_string(statement, 2, doc.metadata.c_str());
            cass_statement_bind_int64(statement, 3, doc.timestamp);

            cass_batch_add_statement(batch, statement);
            cass_statement_free(statement);
        }

        CassFuture* batch_future = cass_session_execute_batch(session, batch);
        CassError rc = cass_future_error_code(batch_future);

        if (rc != CASS_OK) {
            std::cerr << "Error ejecutando batch: " << cass_error_desc(rc) << std::endl;
            error_count += documents.size();
        } else {
            processed_count += documents.size();
        }

        cass_batch_free(batch);
        cass_future_free(batch_future);
    }

public:
    // Obtener estadísticas del indexador
    struct IndexerStats {
        size_t processed_documents;
        size_t error_count;
        size_t pending_tasks;
        double success_rate;
    };

    IndexerStats get_stats() const {
        IndexerStats stats;
        stats.processed_documents = processed_count.load();
        stats.error_count = error_count.load();

        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            stats.pending_tasks = task_queue.size();
        }

        size_t total = stats.processed_documents + stats.error_count;
        stats.success_rate = total > 0 ? (double)stats.processed_documents / total : 0.0;

        return stats;
    }

    // Esperar a que se procesen todas las tareas pendientes
    void wait_for_completion() {
        while (true) {
            {
                std::lock_guard<std::mutex> lock(queue_mutex);
                if (task_queue.empty()) {
                    break;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    void shutdown() {
        stop_flag = true;
        condition.notify_all();

        for (auto& worker : workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

private:
    void cleanup() {
        if (prepared_insert) {
            cass_prepared_free(prepared_insert);
        }
        if (prepared_update) {
            cass_prepared_free(prepared_update);
        }
        if (session) {
            CassFuture* close_future = cass_session_close(session);
            cass_future_wait(close_future);
            cass_future_free(close_future);
            cass_session_free(session);
        }
        if (cluster) {
            cass_cluster_free(cluster);
        }
    }
};

// Ejemplo de uso
int main() {
    try {
        // Inicializar indexador con 8 threads y batches de 1000 documentos
        ScyllaDBIndexer indexer("127.0.0.1", 8, 1000);

        // Crear documentos de ejemplo
        std::vector<ScyllaDBIndexer::IndexDocument> documents;
        for (int i = 0; i < 10000; ++i) {
            documents.emplace_back(
                "doc_" + std::to_string(i),
                "Contenido del documento " + std::to_string(i),
                "metadata_" + std::to_string(i % 100)
            );
        }

        auto start = std::chrono::high_resolution_clock::now();

        // Indexar documentos en paralelo
        std::cout << "Iniciando indexación de " << documents.size() << " documentos..." << std::endl;
        indexer.index_documents_parallel(documents);

        // Esperar completar
        indexer.wait_for_completion();

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        // Mostrar estadísticas
        auto stats = indexer.get_stats();
        std::cout << "\nIndexación completada:" << std::endl;
        std::cout << "- Documentos procesados: " << stats.processed_documents << std::endl;
        std::cout << "- Errores: " << stats.error_count << std::endl;
        std::cout << "- Tasa de éxito: " << (stats.success_rate * 100) << "%" << std::endl;
        std::cout << "- Tiempo total: " << duration.count() << " ms" << std::endl;
        std::cout << "- Throughput: " << (stats.processed_documents * 1000.0 / duration.count()) 
                  << " docs/segundo" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}