#pragma once
// Minimal HTTP/1.1 request-head parser and response writer for the framework
// application host. Pure functions over byte buffers; no Seastar types.
#include <cstddef>
#include <ctime>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace probe::http1 {

enum class Parse { complete, incomplete, bad_request, too_large };

// chunk: one chunk's data ended (feed again for the next).
enum class Chunked { complete, incomplete, bad_request, too_large, chunk };

// Incremental decoder of a chunked request body (RFC 9112 7.1), as strict as
// Sanic's Http.read: extensions are ignored, trailer fields are rejected.
class ChunkedDecoder {
    enum class State { size, data, data_end, trailer } state_ = State::size;
    size_t remaining_ = 0;
public:
    // Decodes from data[pos...] into body, advancing pos past what it used.
    // With one_chunk it stops after each chunk's data (Chunked::chunk).
    Chunked feed(std::string_view data, size_t& pos, std::string& body, size_t max_body,
                 bool one_chunk = false);
};

struct Head {
    std::string_view method, target, version;          // views into the parsed buffer
    std::vector<std::pair<std::string_view, std::string_view>> headers;
    size_t size = 0;            // bytes consumed by the head, including leading blank lines
    size_t content_length = 0;
    bool has_content_length = false;
    bool has_transfer_encoding = false;
    bool chunked = false;  // Transfer-Encoding is exactly "chunked"
    bool keep_alive = true;
    bool expect_continue = false;
    // When parsing fails: the status and message Sanic's Http.http1_request_header
    // reports for the same input (BadRequest, PayloadTooLarge, ExpectationFailed).
    int error_status = 400;
    std::string error;
};

// Parses one request head from the start of `data`. `limit` bounds the head
// size and `max_headers` the field count (both report too_large). Failures set
// head.error_status and head.error; head.target is set when the request line
// was split.
Parse parse_head(std::string_view data, size_t limit, size_t max_headers, Head& head);

bool iequals(std::string_view a, std::string_view b);

// RFC 9110: these statuses never carry content or Content-Length framing here.
inline bool status_has_body(int status) {
    return status >= 200 && status != 204 && status != 304;
}

// "Date: <IMF-fixdate>\r\n", recomputed at most once per second.
class DateLine {
    std::time_t second_ = -1;
    char line_[48] = {};
    size_t size_ = 0;
public:
    std::string_view get();
};

// Appends a complete response. Content-Length is derived from `body`; the
// body is omitted for HEAD and for statuses without content. `date` and
// `extra` are complete header lines (each may be empty).
void append_response(std::string& out, int status, std::string_view body,
                     const std::vector<std::pair<std::string, std::string>>& headers,
                     bool head_request, bool close, std::string_view date, std::string_view extra);

// The status line and headers of a response whose body (body_size bytes) the
// caller writes separately.
void append_head(std::string& out, int status, size_t body_size,
                 const std::vector<std::pair<std::string, std::string>>& headers,
                 bool close, std::string_view date, std::string_view extra);

// The head of a streamed response: `framing` is its Transfer-Encoding or
// Content-Length line (possibly empty), written instead of a computed length.
void append_stream_head(std::string& out, int status,
                        const std::vector<std::pair<std::string, std::string>>& headers,
                        bool close, std::string_view date, std::string_view extra,
                        std::string_view framing);

// The head of a protocol switch (101): exactly the application's headers, as
// Sanic writes the WebSocket handshake response.
void append_upgrade_head(std::string& out, int status,
                         const std::vector<std::pair<std::string, std::string>>& headers);

// A plain-text error response that also closes the connection when asked.
void append_error(std::string& out, int status, bool close, std::string_view date,
                  std::string_view extra);

}  // namespace probe::http1
