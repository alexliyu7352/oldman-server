#pragma once
// Sanic's parse_multipart_form (sanic/request/form.py) over raw bytes, for the
// forms whose result needs no Python codec or Unicode handling: ASCII part
// headers, UTF-8 field values, names present, no RFC 2231 filename*. Anything
// else, including the malformed input Sanic's parser raises on, is declined
// and the caller runs Sanic's parser, so the result is the same either way.
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace probe::multipart {
struct Part {
    std::string name;
    std::optional<std::string> filename;  // a file part
    std::string content_type = "text/plain";
    std::string_view data;
};

namespace detail {
// re.ASCII \s, the whitespace `;\s*` skips.
inline bool regex_space(char c) { return c == ' ' || (c >= '\t' && c <= '\r'); }
// str.isspace() for ASCII, the whitespace str.strip() removes.
inline bool python_space(char c) { return regex_space(c) || (c >= '\x1c' && c <= '\x1f'); }
// [\w!#$%&'*+\-.^_`|~] with re.ASCII.
inline bool token(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
           (c != '\0' && std::strchr("_!#$%&'*+-.^`|~", c) != nullptr);
}
inline std::string lower(std::string_view s) {
    std::string out(s);
    for (auto& c : out) if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
    return out;
}
inline std::string replace_all(std::string s, std::string_view from, std::string_view to) {
    for (size_t at = 0; (at = s.find(from, at)) != std::string::npos; at += to.size())
        s.replace(at, from.size(), to);
    return s;
}
// parse_content_header: the stripped, lowered value and its options in first
// insertion order (a later duplicate replaces the value, as in a dict).
struct Header {
    std::string value;
    std::vector<std::pair<std::string, std::string>> options;
    const std::string* get(std::string_view key) const {
        for (const auto& [name, option] : options) if (name == key) return &option;
        return nullptr;
    }
};
inline Header content_header(std::string_view text) {
    Header header;
    const auto pos = text.find(';');
    if (pos != std::string_view::npos) {
        // _param.finditer(value[pos:]): `;\s*TOKEN=(?:TOKEN|"[^"]*")`. The token
        // set excludes whitespace, '=' and '"', so a linear scan matches as re does.
        const auto s = text.substr(pos);
        size_t i = 0;
        while (i < s.size()) {
            if (s[i] != ';') { ++i; continue; }
            size_t j = i + 1;
            while (j < s.size() && regex_space(s[j])) ++j;
            size_t k = j;
            while (k < s.size() && token(s[k])) ++k;
            if (k == j || k >= s.size() || s[k] != '=') { ++i; continue; }
            size_t v = k + 1, end;
            std::string_view option;
            if (v < s.size() && token(s[v])) {
                end = v;
                while (end < s.size() && token(s[end])) ++end;
                option = s.substr(v, end - v);
            } else if (v < s.size() && s[v] == '"') {
                const auto close = s.find('"', v + 1);
                if (close == std::string_view::npos) { ++i; continue; }
                option = s.substr(v + 1, close - v - 1);
                end = close + 1;
            } else {
                ++i;
                continue;
            }
            auto key = lower(s.substr(j, k - j));
            auto value = replace_all(replace_all(std::string(option), "%22", "\""), "%0D%0A", "\n");
            bool replaced = false;
            for (auto& [name, existing] : header.options)
                if (name == key) { existing = std::move(value); replaced = true; break; }
            if (!replaced) header.options.emplace_back(std::move(key), std::move(value));
            i = end;
        }
        text = text.substr(0, pos);
    }
    size_t first = 0, last = text.size();
    while (first < last && python_space(text[first])) ++first;
    while (last > first && python_space(text[last - 1])) --last;
    header.value = lower(text.substr(first, last - first));
    return header;
}
// Python's codec lookup normalizes these names to UTF-8.
inline bool utf8_charset(std::string_view name) {
    const auto l = lower(name);
    return l == "utf-8" || l == "utf8" || l == "utf_8";
}
}  // namespace detail

// The parts in body order, or nullopt when Sanic's parser must decide.
inline std::optional<std::vector<Part>> parse(std::string_view body, std::string_view boundary) {
    using namespace detail;
    if (boundary.empty()) return std::nullopt;  // bytes.split raises
    // body.split(boundary)[1:-1]: the parts between consecutive boundaries.
    std::vector<size_t> marks;
    for (size_t at = 0;;) {
        const void* found = ::memmem(body.data() + at, body.size() - at, boundary.data(), boundary.size());
        if (!found) break;
        marks.push_back(static_cast<const char*>(found) - body.data());
        at = marks.back() + boundary.size();
    }
    std::vector<Part> parts;
    for (size_t m = 0; m + 1 < marks.size(); ++m) {
        const auto begin = marks[m] + boundary.size();
        const auto part = body.substr(begin, marks[m + 1] - begin);
        Part out;
        std::optional<std::string> name;
        std::string charset = "utf-8";
        size_t line_index = 2;
        for (;;) {
            const auto line_end = line_index <= part.size() ? part.find("\r\n", line_index)
                                                            : std::string_view::npos;
            if (line_end == std::string_view::npos) return std::nullopt;  // Sanic's odd slicing
            const auto line = part.substr(line_index, line_end - line_index);
            line_index = line_end + 2;
            if (line.empty()) break;
            for (unsigned char c : line) if (c >= 0x80) return std::nullopt;  // decode, NFC
            const auto colon = line.find(':');
            if (colon == std::string_view::npos) return std::nullopt;  // ValueError
            const auto field = lower(line.substr(0, colon));
            const auto header = content_header(line.substr(std::min(colon + 2, line.size())));
            if (field == "content-disposition") {
                const auto* named = header.get("name");
                name = named ? std::optional<std::string>(*named) : std::nullopt;
                const auto* filename = header.get("filename");
                out.filename = filename ? std::optional<std::string>(*filename) : std::nullopt;
                const auto* extended = header.get("filename*");
                if (!filename && extended && !extended->empty()) return std::nullopt;  // RFC 2231
            } else if (field == "content-type") {
                out.content_type = header.value;
                const auto* given = header.get("charset");
                charset = given ? *given : "utf-8";
            }
        }
        if (!name || name->empty()) return std::nullopt;  // Sanic logs and skips the part
        if (!out.filename && !utf8_charset(charset)) return std::nullopt;
        // form_part[line_index:-4], with Python's slice clamping.
        const auto stop = part.size() >= 4 ? part.size() - 4 : 0;
        out.data = line_index < stop ? part.substr(line_index, stop - line_index) : std::string_view{};
        out.name = std::move(*name);
        parts.push_back(std::move(out));
    }
    return parts;
}
}  // namespace probe::multipart
