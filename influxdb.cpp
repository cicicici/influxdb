#include <influxdb/influxdb.hpp>


namespace influxdb_cpp {

    int query(std::string& resp, const std::string& query, const server_info& si) {
        std::string qs("&q=");
        detail::inner::url_encode(qs, query);
        return detail::inner::http_request("GET", "query", qs, "", si, &resp);
    }

    int create_db(std::string& resp, const std::string& db_name, const server_info& si) {
        std::string qs("&q=create+database+");
        detail::inner::url_encode(qs, db_name);
        return detail::inner::http_request("POST", "query", qs, "", si, &resp);
    }

    namespace detail {

        unsigned char inner::to_hex(unsigned char x) {
            return  x > 9 ? x + 55 : x + 48;
        }

        void inner::url_encode(std::string& out, const std::string& src) {
            size_t pos = 0, start = 0;
            while((pos = src.find_first_not_of("abcdefghijklmnopqrstuvwxyqABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.~", start)) != std::string::npos) {
                out.append(src.c_str() + start, pos - start);
                if(src[pos] == ' ')
                    out += "+";
                else {
                    out += '%';
                    out += to_hex((unsigned char)src[pos] >> 4);
                    out += to_hex((unsigned char)src[pos] & 0xF);
                }
                start = ++pos;
            }
            out.append(src.c_str() + start, src.length() - start);
        }

        int inner::http_request(const char* method, const char* uri,
            const std::string& querystring, const std::string& body, const server_info& si, std::string* resp) {
            std::string header;
            struct iovec iv[2];
            struct sockaddr_in addr;
            int sock, ret_code = 0, content_length = 0, len = 0;
            char ch;
            unsigned char chunked = 0;

            addr.sin_family = AF_INET;
            addr.sin_port = htons(si.port_);
            if((addr.sin_addr.s_addr = inet_addr(si.host_.c_str())) == INADDR_NONE) return -1;

            if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) return -2;

            if(connect(sock, (struct sockaddr*)(&addr), sizeof(addr)) < 0) {
                closesocket(sock);
                return -3;
            }

            header.resize(len = 0x100);

            for(;;) {
                iv[0].iov_len = snprintf(&header[0], len,
                    "%s /%s?db=%s&u=%s&p=%s%s HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\n\r\n",
                    method, uri, si.db_.c_str(), si.usr_.c_str(), si.pwd_.c_str(),
                    querystring.c_str(), si.host_.c_str(), (int)body.length());
                if((int)iv[0].iov_len >= len)
                    header.resize(len *= 2);
                else
                    break;
            }
            iv[0].iov_base = &header[0];
            iv[1].iov_base = (void*)&body[0];
            iv[1].iov_len = body.length();

            if(writev(sock, iv, 2) < (int)(iv[0].iov_len + iv[1].iov_len)) {
                ret_code = -6;
                goto END;
            }

            iv[0].iov_len = len;

#define _NO_MORE() (len >= (int)iv[0].iov_len && \
    (iv[0].iov_len = recv(sock, &header[0], header.length(), len = 0)) == size_t(-1))
#define _GET_NEXT_CHAR() (ch = _NO_MORE() ? 0 : header[len++])
#define _LOOP_NEXT(statement) for(;;) { if(!(_GET_NEXT_CHAR())) { ret_code = -7; goto END; } statement }
#define _UNTIL(c) _LOOP_NEXT( if(ch == c) break; )
#define _GET_NUMBER(n) _LOOP_NEXT( if(ch >= '0' && ch <= '9') n = n * 10 + (ch - '0'); else break; )
#define _GET_CHUNKED_LEN(n, c) _LOOP_NEXT( if(ch >= '0' && ch <= '9') n = n * 16 + (ch - '0'); \
            else if(ch >= 'A' && ch <= 'F') n = n * 16 + (ch - 'A') + 10; \
            else if(ch >= 'a' && ch <= 'f') n = n * 16 + (ch - 'a') + 10; else {if(ch != c) { ret_code = -8; goto END; } break;} )
#define _(c) if((_GET_NEXT_CHAR()) != c) break;
#define __(c) if((_GET_NEXT_CHAR()) != c) { ret_code = -9; goto END; }

            if(resp) resp->clear();

            _UNTIL(' ')_GET_NUMBER(ret_code)
            for(;;) {
                _UNTIL('\n')
                switch(_GET_NEXT_CHAR()) {
                    case 'C':_('o')_('n')_('t')_('e')_('n')_('t')_('-')
                        _('L')_('e')_('n')_('g')_('t')_('h')_(':')_(' ')
                        _GET_NUMBER(content_length)
                        break;
                    case 'T':_('r')_('a')_('n')_('s')_('f')_('e')_('r')_('-')
                        _('E')_('n')_('c')_('o')_('d')_('i')_('n')_('g')_(':')
                        _(' ')_('c')_('h')_('u')_('n')_('k')_('e')_('d')
                        chunked = 1;
                        break;
                    case '\r':__('\n')
                        switch(chunked) {
                            do {__('\r')__('\n')
                            case 1:
                                _GET_CHUNKED_LEN(content_length, '\r')__('\n')
                                if(!content_length) {
                                    __('\r')__('\n')
                                    goto END;
                                }
                            case 0:
                                while(content_length > 0 && !_NO_MORE()) {
                                    content_length -= (iv[1].iov_len = std::min(content_length, (int)iv[0].iov_len - len));
                                    if(resp) resp->append(&header[len], iv[1].iov_len);
                                    len += iv[1].iov_len;
                                }
                            } while(chunked);
                        }
                        goto END;
                }
                if(!ch) {
                    ret_code = -10;
                    goto END;
                }
            }
            ret_code = -11;
        END:
            closesocket(sock);
            return ret_code / 100 == 2 ? 0 : ret_code;
#undef _NO_MORE
#undef _GET_NEXT_CHAR
#undef _LOOP_NEXT
#undef _UNTIL
#undef _GET_NUMBER
#undef _GET_CHUNKED_LEN
#undef _
#undef __
        }
    }
}

