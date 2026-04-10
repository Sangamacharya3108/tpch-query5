#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <chrono>
#include <unordered_map>
#include <unordered_set>

// Helper: parse one line into a map using column names
static std::map<std::string,std::string>
parseLine(const std::string& line, const std::vector<std::string>& cols) {
    std::map<std::string,std::string> row;
    std::istringstream ss(line);
    std::string field;
    for (const auto& col : cols) {
        if (!std::getline(ss, field, '|')) break;
        row[col] = field;
    }
    return row;
}

// Helper: load one .tbl file into a vector of maps
static bool loadTable(const std::string& filepath,
    const std::vector<std::string>& columns,
    std::vector<std::map<std::string,std::string>>& out)
{
    std::ifstream f(filepath);
    if (!f.is_open()) {
        std::cerr << "Cannot open: " << filepath << std::endl;
        return false;
    }
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        while (!line.empty() && (line.back()=='\r' || line.back()=='|'))
            line.pop_back();
        out.push_back(parseLine(line, columns));
    }
    return true;
}

bool parseArgs(int argc, char* argv[],
    std::string& r_name, std::string& start_date,
    std::string& end_date, int& num_threads,
    std::string& table_path, std::string& result_path)
{
    for (int i = 1; i < argc - 1; ++i) {
        std::string key = argv[i];
        std::string val = argv[i + 1];
        if (key == "--r_name")       { r_name = val; ++i; }
        else if (key == "--start_date") { start_date = val; ++i; }
        else if (key == "--end_date")   { end_date = val; ++i; }
        else if (key == "--threads")    { num_threads = std::stoi(val); ++i; }
        else if (key == "--table_path") { table_path = val; ++i; }
        else if (key == "--result_path"){ result_path = val; ++i; }
    }
    if (r_name.empty() || start_date.empty() || end_date.empty() ||
        num_threads <= 0 || table_path.empty() || result_path.empty())
        return false;
    return true;
}

bool readTPCHData(const std::string& table_path,
    std::vector<std::map<std::string,std::string>>& customer_data,
    std::vector<std::map<std::string,std::string>>& orders_data,
    std::vector<std::map<std::string,std::string>>& lineitem_data,
    std::vector<std::map<std::string,std::string>>& supplier_data,
    std::vector<std::map<std::string,std::string>>& nation_data,
    std::vector<std::map<std::string,std::string>>& region_data)
{
    std::vector<std::string> customer_cols = {
        "c_custkey","c_name","c_address","c_nationkey",
        "c_phone","c_acctbal","c_mktsegment","c_comment"};
    std::vector<std::string> orders_cols = {
        "o_orderkey","o_custkey","o_orderstatus","o_totalprice",
        "o_orderdate","o_orderpriority","o_clerk","o_shippriority","o_comment"};
    std::vector<std::string> lineitem_cols = {
        "l_orderkey","l_partkey","l_suppkey","l_linenumber",
        "l_quantity","l_extendedprice","l_discount","l_tax",
        "l_returnflag","l_linestatus","l_shipdate","l_commitdate",
        "l_receiptdate","l_shipinstruct","l_shipmode","l_comment"};
    std::vector<std::string> supplier_cols = {
        "s_suppkey","s_name","s_address","s_nationkey",
        "s_phone","s_acctbal","s_comment"};
    std::vector<std::string> nation_cols = {
        "n_nationkey","n_name","n_regionkey","n_comment"};
    std::vector<std::string> region_cols = {
        "r_regionkey","r_name","r_comment"};

    std::cout << "Loading tables..." << std::endl;
    if (!loadTable(table_path+"/customer.tbl", customer_cols, customer_data)) return false;
    std::cout << " customer : " << customer_data.size() << " rows" << std::endl;
    if (!loadTable(table_path+"/orders.tbl", orders_cols, orders_data)) return false;
    std::cout << " orders   : " << orders_data.size() << " rows" << std::endl;
    if (!loadTable(table_path+"/lineitem.tbl", lineitem_cols, lineitem_data)) return false;
    std::cout << " lineitem : " << lineitem_data.size() << " rows" << std::endl;
    if (!loadTable(table_path+"/supplier.tbl", supplier_cols, supplier_data)) return false;
    std::cout << " supplier : " << supplier_data.size() << " rows" << std::endl;
    if (!loadTable(table_path+"/nation.tbl", nation_cols, nation_data)) return false;
    std::cout << " nation   : " << nation_data.size() << " rows" << std::endl;
    if (!loadTable(table_path+"/region.tbl", region_cols, region_data)) return false;
    std::cout << " region   : " << region_data.size() << " rows" << std::endl;
    return true;
}

// Thread worker function
static void processChunk(
    const std::vector<std::map<std::string,std::string>>& lineitem_data,
    size_t start, size_t end,
    const std::unordered_map<std::string,std::string>& order_to_custkey,
    const std::unordered_map<std::string,std::string>& custkey_to_nation,
    const std::unordered_map<std::string,std::string>& suppkey_to_nation,
    const std::unordered_map<std::string,std::string>& nation_id_to_name,
    const std::unordered_set<std::string>& valid_order_keys,
    std::map<std::string,double>& local_result)
{
    for (size_t i = start; i < end; ++i) {
        const auto& li = lineitem_data[i];
        const std::string& l_orderkey = li.at("l_orderkey");
        const std::string& l_suppkey  = li.at("l_suppkey");

        // Filter: order must be in valid date range
        if (!valid_order_keys.count(l_orderkey)) continue;

        // Get custkey from orderkey
        auto it_c = order_to_custkey.find(l_orderkey);
        if (it_c == order_to_custkey.end()) continue;

        // Get customer nation
        auto it_cn = custkey_to_nation.find(it_c->second);
        if (it_cn == custkey_to_nation.end()) continue;
        const std::string& cust_nation = it_cn->second;

        // Get supplier nation
        auto it_s = suppkey_to_nation.find(l_suppkey);
        if (it_s == suppkey_to_nation.end()) continue;

        // KEY CONDITION: customer and supplier must be in SAME nation
        if (cust_nation != it_s->second) continue;

        // Get nation name
        auto it_n = nation_id_to_name.find(cust_nation);
        if (it_n == nation_id_to_name.end()) continue;

        // Accumulate revenue = extendedprice * (1 - discount)
        if (li.at("l_extendedprice").empty()) continue;
        double ext = std::stod(li.at("l_extendedprice"));
        double dis = std::stod(li.at("l_discount"));
        local_result[it_n->second] += ext * (1.0 - dis);
    }
}

bool executeQuery5(const std::string& r_name,
    const std::string& start_date, const std::string& end_date,
    int num_threads,
    const std::vector<std::map<std::string,std::string>>& customer_data,
    const std::vector<std::map<std::string,std::string>>& orders_data,
    const std::vector<std::map<std::string,std::string>>& lineitem_data,
    const std::vector<std::map<std::string,std::string>>& supplier_data,
    const std::vector<std::map<std::string,std::string>>& nation_data,
    const std::vector<std::map<std::string,std::string>>& region_data,
    std::map<std::string,double>& results)
{
    auto t_start = std::chrono::high_resolution_clock::now();

    // Step 1: Find regionkey for r_name
    std::unordered_set<std::string> asia_regionkeys;
    for (const auto& row : region_data)
        if (row.at("r_name") == r_name)
            asia_regionkeys.insert(row.at("r_regionkey"));

    // Step 2: Find nations in that region; build nationkey->name map
    std::unordered_map<std::string,std::string> nation_id_to_name;
    for (const auto& row : nation_data)
        if (asia_regionkeys.count(row.at("n_regionkey")))
            nation_id_to_name[row.at("n_nationkey")] = row.at("n_name");

    // Step 3: Build custkey->nationkey for ASIA customers only
    std::unordered_map<std::string,std::string> custkey_to_nation;
    for (const auto& row : customer_data)
        if (nation_id_to_name.count(row.at("c_nationkey")))
            custkey_to_nation[row.at("c_custkey")] = row.at("c_nationkey");

    // Step 4: Build suppkey->nationkey for ASIA suppliers only
    std::unordered_map<std::string,std::string> suppkey_to_nation;
    for (const auto& row : supplier_data)
        if (nation_id_to_name.count(row.at("s_nationkey")))
            suppkey_to_nation[row.at("s_suppkey")] = row.at("s_nationkey");

    // Step 5: Filter orders by date range AND ASIA customer
    std::unordered_set<std::string> valid_order_keys;
    std::unordered_map<std::string,std::string> order_to_custkey;
    for (const auto& row : orders_data) {
        const std::string& odate = row.at("o_orderdate");
        if (odate >= start_date && odate < end_date) {
            const std::string& ck = row.at("o_custkey");
            if (custkey_to_nation.count(ck)) {
                valid_order_keys.insert(row.at("o_orderkey"));
                order_to_custkey[row.at("o_orderkey")] = ck;
            }
        }
    }

    std::cout << "Preprocessing complete." << std::endl;
    std::cout << " ASIA nations   : " << nation_id_to_name.size() << std::endl;
    std::cout << " ASIA customers : " << custkey_to_nation.size() << std::endl;
    std::cout << " ASIA suppliers : " << suppkey_to_nation.size() << std::endl;
    std::cout << " Valid orders   : " << valid_order_keys.size() << std::endl;
    std::cout << " Lineitem rows  : " << lineitem_data.size() << std::endl;

    // Step 6: Divide lineitem into chunks and spawn threads
    size_t total = lineitem_data.size();
    size_t chunk = total / num_threads;
    std::vector<std::map<std::string,double>> partial(num_threads);
    std::vector<std::thread> threads;

    for (int t = 0; t < num_threads; ++t) {
        size_t s = t * chunk;
        size_t e = (t == num_threads-1) ? total : s + chunk;
        threads.emplace_back(processChunk,
            std::cref(lineitem_data), s, e,
            std::cref(order_to_custkey),
            std::cref(custkey_to_nation),
            std::cref(suppkey_to_nation),
            std::cref(nation_id_to_name),
            std::cref(valid_order_keys),
            std::ref(partial[t]));
    }

    // Step 7: Join all threads
    for (auto& th : threads) th.join();

    // Step 8: Merge results
    for (const auto& p : partial)
        for (const auto& kv : p)
            results[kv.first] += kv.second;

    auto t_end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(t_end - t_start).count();
    std::cout << "Query executed in " << elapsed << " s using "
              << num_threads << " thread(s)" << std::endl;
    return true;
}

bool outputResults(const std::string& result_path,
    const std::map<std::string,double>& results)
{
    // Sort by revenue descending
    std::vector<std::pair<std::string,double>> sorted(results.begin(), results.end());
    std::sort(sorted.begin(), sorted.end(),
        [](const auto& a, const auto& b){ return a.second > b.second; });

    // Print to stdout
    std::cout << std::string(45, '=') << std::endl;
    std::cout << "n_name | revenue" << std::endl;
    std::cout << std::string(45, '=') << std::endl;
    for (const auto& kv : sorted)
        std::cout << kv.first << " | " << std::fixed << kv.second << "\n";
    std::cout << std::string(45, '=') << std::endl;

    // Write to file
    std::ofstream f(result_path + "/query5_result.txt");
    if (!f.is_open()) {
        std::cerr << "Cannot open result file" << std::endl;
        return false;
    }
    f << "n_name|revenue\n";
    for (const auto& kv : sorted)
        f << kv.first << "|" << std::fixed << kv.second << "\n";
    return true;
}
