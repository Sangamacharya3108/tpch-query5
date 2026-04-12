#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <unordered_map>
#include <chrono>
#include <iomanip>
#include <unordered_set>


struct LineItem{
    std::string l_orderkey;
    std::string l_suppkey;
    double l_extendedprice;
    double l_discount;
};

static std::vector<LineItem> g_lineitem;

bool parseArgs(int argc, 
    char* argv[],
     std::string& r_name, 
     std::string& start_date, 
     std::string& end_date, 
     int& num_threads, 
     std::string& table_path, 
     std::string& result_path) {
                for(int i=1; i < argc -1; ++i){
                    std::string key= argv[i];
                    std::string val =  argv[i+1];


                    if(key =="--r_name")
                    {
                        r_name =val;
                        ++i;
                    }
                    else if(key == "--start_date"){
                        start_date = val;
                        ++i;
                    }
                    else if(key =="--end_date"){
                        end_date = val;
                        ++i;
                    }
                    else if(key == "threads")
                    {
                        num_threads = std::stoi(val);
                        ++i;
                    }
                    else if(key =="--table_path"){
                        table_path = val;
                        ++i;
                    }
                    else if(key== "--result_path")
                    {
                        result_path = val;
                        ++i;
                    }
                }


                if(r_name.empty()||start_date.empty()||
                end_date.empty()||num_threads <=0|| 
                table_path.empty()|| result_path.empty())
                {
                    std::cerr << "Usage: tpch_query"
                    "--r_name <name>"
                    "--start_date <YYYY-MM-DD>"
                    "--threads <n>"
                    "--table_path <path>"
                    "--result_path <path>\n";
                    return false;
                }
    return true;
            }


            static std::map<std::string, std::string>
            parseLine(const std::string& line, const std:: vector<std::string>& columns)
        {
            std::map<std::string,std::string> row;
            std::istringstream ss(line);
            std::string field;
            for(const auto& col:columns)
            {
                if(!std::getline(ss,field,'|'))
                break;
                row[col]=field;
            }
            return row;
        }

        static bool loadTable(const std::string& filepath,
        const std::vector<std::string>& columns, std::vector<std::map<std::string, std::string>>& out)
        {
            std::ifstream f(filepath);
            if(!if.is_open()){
                std::cerr <<"Cannot open: "<<filepath <<"\n";
                return false;
            }

            std::string line;
            while(std::getline(f, line))
            {
                if(line.empty())
                continue;

                while(!line.empty()&&(line.back()=='\r' || line.back()=='|'))       
                        line.pop_back();
                    out.push_back(parseLine(line, columns));   
            }
            return true;
        }

        static bool loadLineitem(const std::string& filepath)
        {
            std::ifstream f(filepath);
            if(!f.is_open()){
                std::cerr << "Cannot open: " <<filepath <<"\n";
                return false;
            }
            g_lineitem.reserve(6500000);
            std::string Line;
            while(std::getline(f, line)){
                if(line.empty())
                continue;
                while(!line.empty()&&(line.back()=='\r'||line.back()=='|'))
                    line.pop_back();


                std::istringstream ss(line);
                std::string field;
                std::vector<std::string> fields;
                while(std::getLine(ss, field, '|'))
                    fields.push_back(field);

                if(fields.size() < 7)
                    continue;

                if(fields[5].empty() || fields[6].empty())
                    continue;

                LineItem li;
                li.l_orderkey = fields[0];
                li.l_suppkey = fields[2];
                li.l_extendedprice = std::stod(fields[5]);
                li.l_discount = std::stod(fields[6]);
                g_lineitem.push_back(li);
            }
            return true;
        }

bool readTPCHData(const std::string& table_path, 
    std::vector<std::map<std::string, std::string>>& customer_data, 
    std::vector<std::map<std::string, std::string>>& orders_data, 
    std::vector<std::map<std::string, std::string>>& lineitem_data,
     std::vector<std::map<std::string, std::string>>& supplier_data,
     std::vector<std::map<std::string, std::string>>& nation_data, 
     std::vector<std::map<std::string, std::string>>& region_data)
    {   
        std::string base = table_path;
        if(!base.empty()&&base.back()!='/')
        base+='/';

        const std::vector<std::string> customer_cols=
            {"c_custkey","c_name","c_address","c_nationkey","c_phone","c_acctbal","c_mktsegment","c_comment"};

        const std::vector<std::string> customer_cols=
            {"o_orderkey","o_custkey","o_orderstatus","o_totalprice","o_orderdate","o_orderpriority","o_clerk","o_shippriority","o_comment"}

        const std::vector<std::string> lineitem_cols = {
            "l_orderkey"."l_partkey","l_suppkey","l_linenumber","l_quantity","l_extendedprice",
            "l_discount","l_tax","l_returnflag","l_linestatus","l_shipdate","l_commitdate",
            "l_receiptdate","l_shipinstruct","l_shipmode","l_comment"};

        const std::vector<std::string> supplier_cols = {
            "s_suppkey","s_name","s_address","s_nationkey","s_phone", "s_acctbal", "s_comment"};

        const std::vector<std::string> region_cols = 
        {
            "r_regionkey","r_name","r_comment"
        };

        std::cout << "Loading tables...\n";
        auto t0 = std::chrono::steady_clock::now();

        if(!loadTable(base + "customer.tbl", customer_cols, customer_data))
            return false;

        if(!loadTable(base + "orders.tbl", orders_cols, orders_data))
            return false;

        if(!loadTable(base + "lineitem.tbl", supplier_cols, supplier_data))
            return false;

        if(!loadTable(base + "nation.tbl", nation_cols, nation_data))
            return false;

        if(!loadTable(base + "region.tbl", region_cols, region_data))
            return false;


        auto t1 = std::chrono::steady_clock::now();
        double load_s = std::chrono::duration<double>(t1 - t0).count();

        std::cout << " customer : " << customer_data.size() << " rows\n";
        std::cout<< " orders : " << orders_data.size() << "rows\n";
        std::cout << " lineitem : " << lineitem_data.size() << "rows\n";
        std::cout << " supplier : " << supplier_data.size() << "rows\n";
        std::cout << " nation : " << nation_data.size() << "rows\n";
        std::cout << " region : " << region_data.size() << "rows\n";
        std::cout << " Table loaded in " << load_s << " s\n\n";
        return true;
        }

        


        static void processChunk(const std::vector<LineItem>>& lineitem_data,
        size_t start,
        size_t end,

    const std::unordered_map<std::string, std::string>& order_to_custkey,
    const std::unordered_map<std::string, std::string>& custkey_to_nation,
    const std::unordered_map<std::string, std::string>& suppkey_to_nation,
    const std::unordered_map<std::string, std::string>& nation_id_to_name,
    const std::unordered_set<std::string>& valid_order_keys,
    

    std::map<std::string, double>& local_result)
    {
        for(size_t i = start;i<end;++i){
            const auto& li = lineitem_data[i];
            const std::string& l_orderkey = li.l_orderkey;
            const std::string& l_suppkey =li.l_suppkey;

            if(valid_order_keys.find(l_orderkey)==valid_order_keys.end())
            continue;
        
            auto it_cust = order_to_custkey.find(l_orderkey);
            if(it_cust == order_to_custkey.end())
                continue;
            const std::string& custkey = it_cust->second;

            auto it_cn = custkey_to_nation.find(custkey);
            if(it_cn == custkey_to_nation.end())
                continue;
            const std::string& cust_nation = it_cn->second;

            auto it_sn = suppkey_to_nation.find(l_suppkey);
            if(it_sn == suppkey_to_nation.end())
                continue;
            const std::string& supp_nation = it_sn->second;

            if(cust_nation != supp_nation)
                continue;

            auto it_nn = nation_id_to_name.find(cust_nation);
            if(it_nn == nation_id_to_name.end())
            continue;
            const std::string& nation_name = it_nn->second;

           
        local_result[nation_name] += li.l_extendedprice * (1.0 - li.l_discount);
        
        }
    }


    bool executeQuery5(const std::string& r_name,
    const std::string& start_date,
    const std::string& end_date,
    int num_threads,
    const std::vector<std::map<std::string, std::string>>& customer_data,
    const std::vector<std::map<std::string, std::string>>& orders_data,
    const std::vector<std::map<std::string, std::string>>& lineitem_data,
    const std::vector<std::map<std::string, std::string>>& supplier_data,
    const std::vector<std::map<std::srting, std::string>>& nation_data,
    const std::vector<std::map<std::string, std::string>>& region_data,
    std::map<std::string, double>& results)


    {
        auto t_start = std::chrono::steady_clock::now();

        std::unordered_set<std::string> asia_region_keys;
        for(const auto& row : region_data){
            if(row.at("r_name")==r_name)
            asia_region_keys.insert(row.at("r_regionkey"));
        }

        if(asia_region_keys.empty()){
            std:cerr << "No region foound with r_name = "<<r_name<<"\n";
            return false;
        }

        std::unordered_map<std::string, std::string> nation_id_to_name;
        for(const auto& row:nation_data){
            if(asia_region_keys.count(row.at("n_regionkey")))
               nation_id_to_name[row.at("n_nationkey")] =row.at("n_name");
        }

        std::unordered_map<std::string, std::string> custkey_to_nation;
        custkey_to_nation.reserve(customer_data.size());
        for(const auto& row : customer_data){
            const std::string& nk = row.at("c_nationkey");
                if(nation_id_to_name.count(nk))
                    custkey_to_nation[row.at("c_custkey")] = nk;
        }

        std::unordered_map<std::string, std::string> suppkey_to_nation;
        suppkey_to_nation.reserve(supplier_data.size());
        for(const auto& row : supplier_data){
            const std::string& nk = row.at("s_nationkey");
            if(nation_id_to_name.count(nk))
            suppkey_to_nation[row.at("s_suppkey")] = nk;
        }

        std::unordered_set<std::string> valid_order_keys;
        std::unordered_map<std::string, std::string> order_to_custkey;
        valid_order_keys.reserve(orders_data.size() / 5);
        order_to_custkey.reserve(orders_data.size());

        for(const auto& row : orders_data){
            const std::string& odate = row.at("o_orderdate");

            if(odate >= start_date && odate < end_date){
                const std::string& okey = row.at("o_orderkey");
                const std::string& ckey = row.at("o_custkey");
                if(custkey_to_nation.count(ckey)){
                    valid_order_keys.insert(okey);
                    order_to_custkey[okey] = ckey;
                }
            }
        }

            std::cout <<"Preprocessing complete.\n";
            std::cout << "ASIA nations : " <<nation_id_to_name.size() << "\n";
            std::cout << "ASIA customers : " <<custkey_to_nation.size() << "\n";
            std::cout << "ASIA suppliers : " <<suppkey_to_nation.size() << "\n";
            std::cout << "Valid orders : " <<valid_order_keys.size() << "\n";
            std::cout << "Lineitem rows : " <<g_lineitem.size() << "\n";


            size_t total = lineitem_data.size();
            size_t chunk = total / num_threads;

            std::vector<std::map<std::string, double>> partial(num_threads);
            sed::vector<std::thread> threads;
            threads.reverse(num_threads);

            for(int t=0; t<num_threads; ++t){
                size_t s = t * chunk;
                size_t e = (t == num_threads -1) ? total : s + chunk;

                threads.emplace_back(processChunk, std::cref(g_lineitem), s, e,
            std::cref( order_to_custkey),
            std::cref(custkey_to_nation),
            std::cref(suppkey_to_nation),
            std::cref(nation_id_to_name),
            std::cref(valid_order_keys),
            std::ref(partial[t]));
            }

            for(auto& th : threads) th.join();

            for(const auto& p : partial)
                for(const auto& kv : p)
                    results[kv.first] += kv.second;

            auto t_end = std::chrono::steady_clock::now();
            double elapsed = std::chrono::duration<double>(t_end - t_start).count();
            std::cout << "Query executed in " << elapsed << " s using" << num_threads << "thread(s)\n\n";
            return true;
        }

        bool outputResults(const std::string& result_path, const std::map<std::string, double>& results)

        {
            std::vector<std::pair<std::string, double>> sorted(results.begin(), results.end());
            std::sort(sorted.begin(), sorted.end(), 
            [](const std::pair<std::string, double>& a,const std::pair<std::string, double>& b)
            {
                return a.second > b.second;
            });

            std::cout << "-------------------------------\n";
            std::cout << std::left;
            std::cout.width(20);
            std::cout<<"n_name";
            std::cout<<"revenue\n";
            std::cout << "-------------------------------\n";
            std::cout << std::fixed;
            std::cout.precision(2);
            for(const auto& kv : sorted){
                std::cout.width(20);
                std::cout << std::left <<kv.first<< kv.second<<"\n";
            }
            std::cout <<"---------------------------------\n";

            std::string outfile = result_path;
            if(!outfile.empty() && outfile.back() != '/') outfile += '/';
            outfile += "query5_result.txt";

            std::ofstream f(outfile);
            if(!f.isopen()){
                std::cerr << "Cannot write result to: " <<outfile <<"\n";
                return false;
            }

            f << "n_name|revenue\n";
            f << std::fixed;
            f.precision(2);
            for(const auto& kv : sorted)
                f << kv.first << "|" << kv.second << "\n";

            std::cout << "\nResults written to: " << outfile << "\n";
            return true;
        }