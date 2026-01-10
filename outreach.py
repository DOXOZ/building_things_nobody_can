import os
import re
import json
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from dotenv import load_dotenv


def load_env() -> None:
    # Load environment variables from .env if present
    load_dotenv(override=False)

def get_clickhouse_client():
    import clickhouse_connect

    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    username = os.getenv("CLICKHOUSE_USERNAME", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    database = os.getenv("CLICKHOUSE_DATABASE", "default")

    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
    )


EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")


def is_email(value: Optional[str]) -> bool:
    if not value:
        return False
    value = value.strip()
    if "@" not in value:
        return False
    return bool(EMAIL_REGEX.match(value))


def pick_email_from_row(row: Dict[str, Any]) -> Optional[str]:
    # Prefer common email/contact field names
    for key in ("email", "email_address", "contact", "contact_email"):
        if key in row and is_email(str(row[key])):
            return str(row[key]).strip()

    # Fallback: scan string-like fields for email
    for k, v in row.items():
        try:
            s = str(v)
        except Exception:
            continue
        if is_email(s):
            return s.strip()
    return None


def build_prompt(person: Dict[str, Any], email: str) -> str:
    name = str(person.get("name") or person.get("full_name") or "").strip()
    company = str(person.get("company") or person.get("organization") or "").strip()

    context_lines: List[str] = []
    if name:
        context_lines.append(f"Name: {name}")
    if company:
        context_lines.append(f"Company: {company}")
    context = "\n".join(context_lines)

    goal = os.getenv(
        "OUTREACH_GOAL",
        "A short, friendly email proposing a quick call, no spam",
    )

    return (
        "Generate a personalized outreach email in English.\n"
        "Return strictly in JSON with fields: subject, body.\n"
        "Requirements:\n"
        "- Subject up to 8 words, no clickbait.\n"
        "- Polite, concrete tone; no fluff or marketing clichés.\n"
        "- 3–6 short sentences, with a clear single CTA.\n"
        "- Plain text only, no HTML.\n"
        f"Goal: {goal}\n"
        f"Recipient email: {email}\n"
        f"Context (if any):\n{context}\n"
    )


def get_openai_client():
    try:
        from openai import OpenAI
    except Exception as e:
        raise RuntimeError(
            "Failed to import openai. Install with: pip install openai"
        ) from e

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set in environment/.env")

    return OpenAI(api_key=api_key)


def generate_email(client, model: str, prompt: str) -> Tuple[str, str]:
    # Try JSON response format first
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You write concise, friendly outreach emails without fluff or spam."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            temperature=float(os.getenv("OUTREACH_TEMPERATURE", "0.5")),
            max_tokens=int(os.getenv("OUTREACH_MAX_TOKENS", "500")),
            response_format={"type": "json_object"},
        )
        content = resp.choices[0].message.content or "{}"
        data = json.loads(content)
        subject = str(data.get("subject") or "").strip()
        body = str(data.get("body") or "").strip()
        if subject and body:
            return subject, body
    except Exception:
        # Fall back to plain text and simple parsing
        pass

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "You write concise, friendly outreach emails without fluff or spam."
                ),
            },
            {"role": "user", "content": prompt + "\nReturn the subject on the first line, then a blank line, then the email body."},
        ],
        temperature=float(os.getenv("OUTREACH_TEMPERATURE", "0.5")),
        max_tokens=int(os.getenv("OUTREACH_MAX_TOKENS", "500")),
    )
    content = (resp.choices[0].message.content or "").strip()
    # Heuristic split: first line as subject
    first_line, _, rest = content.partition("\n\n")
    subject = first_line.strip().replace("Subject:", "").replace("Тема:", "").strip()
    body = rest.strip() if rest.strip() else content
    return subject, body


def fetch_rows(client, query: str, limit: Optional[int]) -> Tuple[List[str], List[List[Any]]]:
    result = client.query(query)
    columns = list(result.column_names)
    rows = result.result_rows
    if limit is not None:
        rows = rows[: limit]
    return columns, rows


def to_dicts(columns: List[str], rows: List[List[Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        item = {columns[i]: r[i] for i in range(len(columns))}
        out.append(item)
    return out


def main() -> None:
    load_env()

    ch_client = get_clickhouse_client()

    query = os.getenv("OUTREACH_QUERY", "SELECT * FROM contact_info")
    limit_env = os.getenv("OUTREACH_LIMIT")
    limit = int(limit_env) if (limit_env and limit_env.isdigit()) else None

    columns, rows = fetch_rows(ch_client, query, limit)
    records = to_dicts(columns, rows)

    if not records:
        print("Нет данных из ClickHouse по запросу.")
        return

    oai_client = get_openai_client()
    model = os.getenv("OPENAI_MODEL", os.getenv("OUTREACH_MODEL", "gpt-4o-mini"))

    results: List[Dict[str, Any]] = []

    for rec in records:
        try:
            email = pick_email_from_row(rec)
            if not email:
                results.append(
                    {
                        "status": "skipped_no_email",
                        "error": "Контакт отсутствует или не email",
                        "contact": rec.get("contact") or rec.get("email") or "",
                        **{k: rec.get(k) for k in ("id", "name", "company") if k in rec},
                    }
                )
                continue

            prompt = build_prompt(rec, email)
            subject, body = generate_email(oai_client, model, prompt)

            results.append(
                {
                    "status": "ok",
                    "error": "",
                    "contact": email,
                    "subject": subject,
                    "body": body,
                    **{k: rec.get(k) for k in ("id", "name", "company") if k in rec},
                }
            )
        except Exception as e:
            results.append(
                {
                    "status": "error",
                    "error": str(e),
                    "contact": rec.get("contact") or rec.get("email") or "",
                    **{k: rec.get(k) for k in ("id", "name", "company") if k in rec},
                }
            )

    df = pd.DataFrame(results)
    out_path = os.getenv("OUTREACH_OUTPUT", "outreach_output.csv")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")

    total = len(results)
    ok = sum(1 for r in results if r.get("status") == "ok")
    skipped = sum(1 for r in results if str(r.get("status")).startswith("skipped"))
    errors = total - ok - skipped
    print(
        f"Готово. Всего: {total}, ок: {ok}, пропущено: {skipped}, ошибок: {errors}. Файл: {out_path}"
    )


if __name__ == "__main__":
    main()
