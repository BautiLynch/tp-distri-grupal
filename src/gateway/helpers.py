import json

def parse_article(payload):
    article = json.loads(payload.decode("utf-8"))
    if not isinstance(article, dict):
        raise ValueError("ARTICLE must contain a JSON object")
    return article


def article_text(article, field, default=None):
    value = article.get(field)
    if value is None:
        value = default
    if not isinstance(value, str):
        raise ValueError(f"Article field {field} must be text")
    return value


def section_names(sections):
    if sections is None:
        return ()
    if not isinstance(sections, list):
        raise ValueError("Article sections must be a list")
    names = []
    _collect_section_names(sections, names)
    return tuple(names)


def _collect_section_names(sections, names):
    for section in sections:
        if not isinstance(section, dict):
            raise ValueError("Article section must be an object")
        if section.get("type") != "section":
            continue
        name = section.get("name")
        if name is not None:
            names.append(article_text(section, "name"))
        parts = section.get("has_parts") or []
        if not isinstance(parts, list):
            raise ValueError("Section has_parts must be a list")
        _collect_section_names(parts, names)


def preprocess_article(article):
    name = article_text(article, "name")
    url = article_text(article, "url")
    date_modified = article_text(article, "date_modified")
    language = article.get("in_language")
    if not isinstance(language, dict):
        raise ValueError("Article in_language must contain an identifier")
    language = article_text(language, "identifier")
    image = article.get("image")
    if image is not None and not isinstance(image, dict):
        raise ValueError("Article image must be an object")
    image_url = article_text(image or {}, "content_url", "")
    filtered = (
        name, url, language, date_modified, image_url,
        section_names(article.get("sections")),
    )
    split = None
    if article.get("abstract") is not None:
        split = (url, language, date_modified, article_text(article, "abstract"))
    return filtered, split
