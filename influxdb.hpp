#ifndef __INFLUXDB_CPP_H
#define __INFLUXDB_CPP_H

#include <string>
#include <cstring>
#include <cstdio>

#ifdef _WIN32
    #define NOMINMAX
    #include <windows.h>
    #include <algorithm>
    #pragma comment(lib, "ws2_32")
    typedef struct iovec { void* iov_base; size_t iov_len; } iovec;
    inline __int64 writev(int sock, struct iovec* iov, int cnt) {
        __int64 r = send(sock, (const char*)iov->iov_base, iov->iov_len, 0);
        return (r < 0 || cnt == 1) ? r : r + writev(sock, iov + 1, cnt - 1);
    }
#else
    #include <unistd.h>
    #include <sys/types.h>
    #include <sys/socket.h>
    #include <sys/uio.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
    #define closesocket close
#endif

#define FMT_BUF_LEN 25 // double 24 bytes, int64_t 21 bytes
#define FMT_APPEND(...) \
    { \
        lines_.resize(lines_.length() + FMT_BUF_LEN);\
        lines_.resize(lines_.length() - FMT_BUF_LEN + snprintf(&lines_[lines_.length() - FMT_BUF_LEN], FMT_BUF_LEN, ##__VA_ARGS__)); \
    }

namespace influxdb_cpp {
    struct server_info {
        std::string host_;
        int port_;
        std::string db_;
        std::string usr_;
        std::string pwd_;
        server_info(const std::string& host, int port, const std::string& db = "", const std::string& usr = "", const std::string& pwd = "")
            : host_(host), port_(port), db_(db), usr_(usr), pwd_(pwd) {}
    };
    namespace detail {
        struct meas_caller;
        struct tag_caller;
        struct field_caller;
        struct ts_caller;
        struct inner {
            static int http_request(const char*, const char*, const std::string&, const std::string&, const server_info&, std::string*);
            static unsigned char to_hex(unsigned char x);
            static void url_encode(std::string& out, const std::string& src);
        };
    }

    int query(std::string& resp, const std::string& query, const server_info& si);
    int create_db(std::string& resp, const std::string& db_name, const server_info& si);

    struct builder {
        detail::tag_caller& meas(const std::string& m) {
            lines_.clear();
            lines_.reserve(0x100);
            return _m(m);
        }
    protected:
        detail::tag_caller& _m(const std::string& m) {
            _escape(m, ", ");
            return (detail::tag_caller&)*this;
        }
        detail::tag_caller& _t(const std::string& k, const std::string& v) {
            lines_ += ',';
            _escape(k, ",= ");
            lines_ += '=';
            _escape(v, ",= ");
            return (detail::tag_caller&)*this;
        }
        detail::field_caller& _f_s(char delim, const std::string& k, const std::string& v) {
            lines_ += delim;
            _escape(k, ",= ");
            lines_ += "=\"";
            _escape(v, "\"");
            lines_ += '\"';
            return (detail::field_caller&)*this;
        }
        detail::field_caller& _f_i(char delim, const std::string& k, long long v) {
            lines_ += delim;
            _escape(k, ",= ");
            lines_ += '=';
            FMT_APPEND("%lldi", v);
            return (detail::field_caller&)*this;
        }
        detail::field_caller& _f_f(char delim, const std::string& k, double v, int prec) {
            lines_ += delim;
            _escape(k, ",= ");
            lines_ += '=';
            FMT_APPEND("%.*lf", prec, v);
            return (detail::field_caller&)*this;
        }
        detail::field_caller& _f_b(char delim, const std::string& k, bool v) {
            lines_ += delim;
            _escape(k, ",= ");
            lines_ += '=';
            lines_ += (v ? 't' : 'f');
            return (detail::field_caller&)*this;
        }
        detail::ts_caller& _ts(long long ts) {
            if (ts > 0)
                FMT_APPEND(" %lld", ts);
            return (detail::ts_caller&)*this;
        }
        int _post_http(const server_info& si, std::string* resp) {
            return detail::inner::http_request("POST", "write", "", lines_, si, resp);
        }
        int _send_udp(const std::string& host, int port) {
            int sock, ret = 0;
            struct sockaddr_in addr;

            addr.sin_family = AF_INET;
            addr.sin_port = htons(port);
            if((addr.sin_addr.s_addr = inet_addr(host.c_str())) == INADDR_NONE)
                return -1;

            if((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
                return -2;

            if(sendto(sock, &lines_[0], lines_.length(), 0, (struct sockaddr *)&addr, sizeof(addr)) < (int)lines_.length())
                ret = -3;

            closesocket(sock);
            return ret;
        }
        void _escape(const std::string& src, const char* escape_seq) {
            size_t pos = 0, start = 0;
            while((pos = src.find_first_of(escape_seq, start)) != std::string::npos) {
                lines_.append(src.c_str() + start, pos - start);
                lines_ += '\\';
                lines_ += src[pos];
                start = ++pos;
            }
            lines_.append(src.c_str() + start, src.length() - start);
        }

        std::string lines_;
    };

    namespace detail {
        struct tag_caller : public builder {
            detail::tag_caller& tag(const std::string& k, const std::string& v)         { return _t(k, v); }
            detail::field_caller& field(const std::string& k, const std::string& v)     { return _f_s(' ', k, v); }
            detail::field_caller& field(const std::string& k, bool v)                   { return _f_b(' ', k, v); }
            detail::field_caller& field(const std::string& k, short v)                  { return _f_i(' ', k, v); }
            detail::field_caller& field(const std::string& k, unsigned short v)         { return _f_i(' ', k, v); }
            detail::field_caller& field(const std::string& k, int v)                    { return _f_i(' ', k, v); }
            detail::field_caller& field(const std::string& k, unsigned int v)           { return _f_i(' ', k, v); }
            detail::field_caller& field(const std::string& k, long v)                   { return _f_i(' ', k, v); }
            detail::field_caller& field(const std::string& k, unsigned long v)          { return _f_i(' ', k, v); }
            detail::field_caller& field(const std::string& k, long long v)              { return _f_i(' ', k, v); }
            detail::field_caller& field(const std::string& k, unsigned long long v)     { return _f_i(' ', k, v); }
            detail::field_caller& field(const std::string& k, double v, int prec = 8)   { return _f_f(' ', k, v, prec); }
        private:
            detail::tag_caller& meas(const std::string& m);
        };
        struct ts_caller : public builder {
            detail::tag_caller& meas(const std::string& m)                            { lines_ += '\n'; return _m(m); }
            int post_http(const server_info& si, std::string* resp = NULL)            { return _post_http(si, resp); }
            int send_udp(const std::string& host, int port)                           { return _send_udp(host, port); }
        };
        struct field_caller : public ts_caller {
            detail::field_caller& field(const std::string& k, const std::string& v)     { return _f_s(',', k, v); }
            detail::field_caller& field(const std::string& k, bool v)                   { return _f_b(',', k, v); }
            detail::field_caller& field(const std::string& k, short v)                  { return _f_i(',', k, v); }
            detail::field_caller& field(const std::string& k, unsigned short v)         { return _f_i(',', k, v); }
            detail::field_caller& field(const std::string& k, int v)                    { return _f_i(',', k, v); }
            detail::field_caller& field(const std::string& k, unsigned int v)           { return _f_i(',', k, v); }
            detail::field_caller& field(const std::string& k, long v)                   { return _f_i(',', k, v); }
            detail::field_caller& field(const std::string& k, unsigned long v)          { return _f_i(',', k, v); }
            detail::field_caller& field(const std::string& k, long long v)              { return _f_i(',', k, v); }
            detail::field_caller& field(const std::string& k, unsigned long long v)     { return _f_i(',', k, v); }
            detail::field_caller& field(const std::string& k, double v, int prec = 8)   { return _f_f(',', k, v, prec); }
            detail::ts_caller& timestamp(unsigned long long ts)                         { return _ts(ts); }
        };
    }
}
#undef FMT_BUF_LEN
#undef FMT_APPEND

#endif
