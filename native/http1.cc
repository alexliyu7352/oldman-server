#include "http1.hh"

#include <algorithm>
#include <charconv>
#include <limits>

namespace probe::http1 {
namespace {

bool is_tchar(unsigned char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
           std::string_view("!#$%&'*+-.^_`|~").find(static_cast<char>(c)) != std::string_view::npos;
}

bool is_token(std::string_view value) {
    if (value.empty()) return false;
    for (unsigned char c : value)
        if (!is_tchar(c)) return false;
    return true;
}

std::string_view trim(std::string_view value) {
    while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) value.remove_prefix(1);
    while (!value.empty() && (value.back() == ' ' || value.back() == '\t')) value.remove_suffix(1);
    return value;
}

std::string_view reason(int status) {
    switch (status) {
    case 101: return "Switching Protocols";
    case 200: return "OK"; case 201: return "Created"; case 202: return "Accepted";
    case 203: return "Non-Authoritative Information"; case 204: return "No Content";
    case 205: return "Reset Content"; case 206: return "Partial Content";
    case 300: return "Multiple Choices"; case 301: return "Moved Permanently"; case 302: return "Found";
    case 303: return "See Other"; case 304: return "Not Modified"; case 307: return "Temporary Redirect";
    case 308: return "Permanent Redirect";
    case 400: return "Bad Request"; case 401: return "Unauthorized"; case 402: return "Payment Required";
    case 403: return "Forbidden"; case 404: return "Not Found"; case 405: return "Method Not Allowed";
    case 406: return "Not Acceptable"; case 407: return "Proxy Authentication Required";
    case 408: return "Request Timeout"; case 409: return "Conflict"; case 410: return "Gone";
    case 411: return "Length Required"; case 412: return "Precondition Failed";
    case 413: return "Content Too Large"; case 414: return "URI Too Long";
    case 415: return "Unsupported Media Type"; case 416: return "Range Not Satisfiable";
    case 417: return "Expectation Failed"; case 418: return "I'm a teapot";
    case 421: return "Misdirected Request"; case 422: return "Unprocessable Content";
    case 423: return "Locked"; case 424: return "Failed Dependency"; case 425: return "Too Early";
    case 426: return "Upgrade Required"; case 428: return "Precondition Required";
    case 429: return "Too Many Requests"; case 431: return "Request Header Fields Too Large";
    case 451: return "Unavailable For Legal Reasons";
    case 500: return "Internal Server Error"; case 501: return "Not Implemented";
    case 502: return "Bad Gateway"; case 503: return "Service Unavailable";
    case 504: return "Gateway Timeout"; case 505: return "HTTP Version Not Supported";
    case 507: return "Insufficient Storage"; case 511: return "Network Authentication Required";
    default: return {};
    }
}

void append_status_line(std::string& out, int status) {
    char digits[8];
    auto [end, ec] = std::to_chars(digits, digits + sizeof(digits), status);
    out.append("HTTP/1.1 ");
    out.append(digits, end);
    out.push_back(' ');
    out.append(reason(status));
    out.append("\r\n");
}

void append_length(std::string& out, size_t size) {
    char digits[24];
    auto [end, ec] = std::to_chars(digits, digits + sizeof(digits), size);
    out.append("Content-Length: ");
    out.append(digits, end);
    out.append("\r\n");
}

}  // namespace

bool iequals(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        unsigned char x = a[i], y = b[i];
        if (x >= 'A' && x <= 'Z') x += 'a' - 'A';
        if (y >= 'A' && y <= 'Z') y += 'a' - 'A';
        if (x != y) return false;
    }
    return true;
}

Parse parse_head(std::string_view data, size_t limit, size_t max_headers, Head& head) {
    auto fail = [&](Parse result, int status, std::string message) {
        head.error_status = status;
        head.error = std::move(message);
        return result;
    };
    auto bad = [&] { return fail(Parse::bad_request, 400, "Bad Request"); };
    auto too_large = [&] {
        return fail(Parse::too_large, 413, "Request header exceeds the size limit");
    };
    // RFC 9112 2.2: ignore empty lines received before the request line. They
    // count towards the limit like the head: otherwise a peer sending only
    // CRLF would grow the buffer without end, each read rescanning it all.
    size_t start = 0;
    while (data.size() - start >= 2 && data[start] == '\r' && data[start + 1] == '\n') start += 2;
    const auto end = data.find("\r\n\r\n", start);
    if (end == std::string_view::npos) {
        if (data.size() >= limit) return too_large();
        // A bare LF cannot complete a head; reject it rather than waiting forever.
        for (size_t i = start; i < data.size(); ++i)
            if (data[i] == '\n' && (i == start || data[i - 1] != '\r')) return bad();
        return Parse::incomplete;
    }
    if (end + 4 > limit) return too_large();
    head.size = end + 4;
    const auto text = data.substr(start, end + 2 - start);  // every line keeps its CRLF

    auto line_end = text.find("\r\n");
    const auto line = text.substr(0, line_end);
    const auto sp1 = line.find(' ');
    if (sp1 == std::string_view::npos) return bad();
    const auto sp2 = line.find(' ', sp1 + 1);
    if (sp2 == std::string_view::npos) return bad();
    head.method = line.substr(0, sp1);
    head.target = line.substr(sp1 + 1, sp2 - sp1 - 1);
    head.version = line.substr(sp2 + 1);
    if (!is_token(head.method) || head.target.empty()) return bad();
    bool ascii_target = true;
    for (unsigned char c : head.target) {
        if (c <= 0x20 || c == 0x7f) return bad();
        if (c >= 0x80) ascii_target = false;
    }
    bool http10 = false;
    if (head.version == "HTTP/1.1") {
        http10 = false;
    } else if (head.version == "HTTP/1.0") {
        http10 = true;
    } else {
        return bad();  // Sanic answers other versions with Bad Request too
    }

    bool close = false, keep_alive = false, bad_length = false;
    std::string_view expect;
    size_t pos = line_end + 2;
    while (pos < text.size()) {
        const auto next = text.find("\r\n", pos);
        const auto field = text.substr(pos, next - pos);
        pos = next + 2;
        if (field.front() == ' ' || field.front() == '\t') return bad();  // obs-fold
        const auto colon = field.find(':');
        if (colon == std::string_view::npos) return bad();
        const auto name = field.substr(0, colon);
        if (!is_token(name)) return bad();  // also rejects space before ':'
        const auto value = trim(field.substr(colon + 1));
        for (unsigned char c : value)
            if ((c < 0x20 && c != '\t') || c == 0x7f) return bad();
        if (head.headers.size() == max_headers) return too_large();
        head.headers.emplace_back(name, value);

        const bool length_field = iequals(name, "content-length");
        if (length_field || iequals(name, "transfer-encoding")) {
            // Like Sanic, at most one Content-Length or Transfer-Encoding field.
            if (head.has_content_length || head.has_transfer_encoding) return bad();
            if (length_field) {
                size_t length = 0;
                const auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), length);
                bad_length = value.empty() || ec != std::errc() || ptr != value.data() + value.size();
                head.has_content_length = true;
                head.content_length = length;
            } else {
                // Only the "chunked" coding is decoded.
                head.chunked = iequals(value, "chunked");
                head.has_transfer_encoding = true;
            }
        } else if (iequals(name, "connection")) {
            auto rest = value;
            while (!rest.empty()) {
                const auto comma = rest.find(',');
                const auto option = trim(rest.substr(0, comma));
                if (iequals(option, "close")) close = true;
                else if (iequals(option, "keep-alive")) keep_alive = true;
                rest = comma == std::string_view::npos ? std::string_view{} : rest.substr(comma + 1);
            }
        } else if (iequals(name, "expect")) {
            expect = value;
            head.expect_continue = iequals(value, "100-continue");
        }
    }
    head.keep_alive = !close && (!http10 || keep_alive);
    // In Sanic's order: the URL, then (with a body) Expect and the length.
    if (!ascii_target)
        return fail(Parse::bad_request, 400, "URL may only contain US-ASCII characters.");
    if (head.has_content_length || head.has_transfer_encoding) {
        if (!expect.empty() && !head.expect_continue)
            return fail(Parse::bad_request, 417, "Unknown Expect: " + std::string(expect));
        if (bad_length || (head.has_transfer_encoding && !head.chunked))
            return fail(Parse::bad_request, 400, "Bad content-length");
    }
    return Parse::complete;
}

Chunked ChunkedDecoder::feed(std::string_view data, size_t& pos, std::string& body,
                             size_t max_body, bool one_chunk) {
    for (;;) {
        if (state_ == State::data) {
            const size_t take = std::min(remaining_, data.size() - pos);
            body.append(data.substr(pos, take));
            pos += take;
            remaining_ -= take;
            if (remaining_) return Chunked::incomplete;
            state_ = State::data_end;
            if (one_chunk) return Chunked::chunk;
        }
        const auto end = data.find("\r\n", pos);
        if (end == std::string_view::npos) {
            // Sanic bounds a chunk size line to 64 bytes.
            return data.size() - pos > 64 ? Chunked::bad_request : Chunked::incomplete;
        }
        const auto line = data.substr(pos, end - pos);
        pos = end + 2;
        switch (state_) {
        case State::data_end:
            if (!line.empty()) return Chunked::bad_request;
            state_ = State::size;
            break;
        case State::size: {
            const auto digits = line.substr(0, line.find(';'));
            size_t size = 0;
            const auto [ptr, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), size, 16);
            if (digits.empty() || ec != std::errc() || ptr != digits.data() + digits.size())
                return Chunked::bad_request;
            if (size > max_body - body.size()) return Chunked::too_large;
            if (size == 0) {
                state_ = State::trailer;
            } else {
                remaining_ = size;
                state_ = State::data;
            }
            break;
        }
        case State::trailer:
            // Trailer bytes left on a keep-alive connection could be read as a
            // smuggled request; Sanic rejects any.
            return line.empty() ? Chunked::complete : Chunked::bad_request;
        case State::data:
            break;
        }
    }
}

std::string_view DateLine::get() {
    const auto now = std::time(nullptr);
    if (now != second_) {
        std::tm parts{};
        gmtime_r(&now, &parts);
        size_ = std::strftime(line_, sizeof(line_), "Date: %a, %d %b %Y %H:%M:%S GMT\r\n", &parts);
        second_ = now;
    }
    return {line_, size_};
}

// Http.http1_response_header: the host frames the response itself, replacing
// the application's Content-Length, Transfer-Encoding and Connection.
static bool framing_field(std::string_view name) {
    return (name.size() == 10 && iequals(name, "connection")) ||
           (name.size() == 14 && iequals(name, "content-length")) ||
           (name.size() == 17 && iequals(name, "transfer-encoding"));
}

void append_head(std::string& out, int status, size_t body_size,
                 const std::vector<std::pair<std::string, std::string>>& headers,
                 bool close, std::string_view date, std::string_view extra) {
    append_status_line(out, status);
    for (const auto& [name, value] : headers) {
        if (framing_field(name)) continue;
        out.append(name);
        out.append(": ");
        out.append(value);
        out.append("\r\n");
    }
    out.append(date);
    out.append(extra);
    if (status_has_body(status)) append_length(out, body_size);
    out.append(close ? "Connection: close\r\n" : "Connection: keep-alive\r\n");
    out.append("\r\n");
}

void append_stream_head(std::string& out, int status,
                        const std::vector<std::pair<std::string, std::string>>& headers,
                        bool close, std::string_view date, std::string_view extra,
                        std::string_view framing) {
    append_status_line(out, status);
    for (const auto& [name, value] : headers) {
        if (framing_field(name)) continue;
        out.append(name);
        out.append(": ");
        out.append(value);
        out.append("\r\n");
    }
    out.append(date);
    out.append(extra);
    out.append(framing);
    out.append(close ? "Connection: close\r\n" : "Connection: keep-alive\r\n");
    out.append("\r\n");
}

void append_response(std::string& out, int status, std::string_view body,
                     const std::vector<std::pair<std::string, std::string>>& headers,
                     bool head_request, bool close, std::string_view date, std::string_view extra) {
    append_head(out, status, body.size(), headers, close, date, extra);
    if (status_has_body(status) && !head_request) out.append(body);
}

void append_upgrade_head(std::string& out, int status,
                         const std::vector<std::pair<std::string, std::string>>& headers) {
    append_status_line(out, status);
    for (const auto& [name, value] : headers) {
        out.append(name);
        out.append(": ");
        out.append(value);
        out.append("\r\n");
    }
    out.append("\r\n");
}

void append_error(std::string& out, int status, bool close, std::string_view date,
                  std::string_view extra) {
    static const std::vector<std::pair<std::string, std::string>> text{{"Content-Type", "text/plain"}};
    append_response(out, status, "request rejected\n", text, false, close, date, extra);
}

}  // namespace probe::http1
