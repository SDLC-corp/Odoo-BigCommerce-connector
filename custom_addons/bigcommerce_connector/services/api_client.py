# -*- coding: utf-8 -*-

import json
import logging
import time

import requests

_logger = logging.getLogger(__name__)


class BigCommerceApiClient:
    """Reusable API client for BigCommerce store-scoped requests."""

    DEFAULT_TIMEOUT = 25
    DEFAULT_RETRIES = 3
    DEFAULT_PAGE_LIMIT = 100

    def __init__(self, instance):
        self.instance = instance

    def _get_headers(self):
        """Return request headers for BigCommerce REST calls."""
        return {
            "X-Auth-Token": (self.instance.access_token or "").strip(),
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _build_url(self, path):
        """Build a full store-scoped URL from instance settings and an endpoint path."""
        base = (self.instance.api_base_url or "").strip().rstrip("/")
        store_hash = (self.instance.store_hash or "").strip().strip("/")
        endpoint = (path or "").strip()
        if not endpoint.startswith("/"):
            endpoint = "/%s" % endpoint
        return "%s/%s%s" % (base, store_hash, endpoint)

    def _request(self, method, path, params=None, payload=None, timeout=None, retries=None):
        """Perform an HTTP request against BigCommerce and return response metadata."""
        timeout = timeout or self.DEFAULT_TIMEOUT
        retries = self.DEFAULT_RETRIES if retries is None else max(0, int(retries))
        url = self._build_url(path)
        started = time.monotonic()

        attempt = 0
        while True:
            try:
                response = requests.request(
                    method=method,
                    url=url,
                    headers=self._get_headers(),
                    params=params,
                    json=payload,
                    timeout=timeout,
                )
            except requests.exceptions.Timeout:
                if attempt < retries:
                    attempt += 1
                    time.sleep(min(2**attempt, 5))
                    continue
                return {
                    "success": False,
                    "status_code": None,
                    "message": "Connection to BigCommerce timed out.",
                    "url": url,
                    "response_body": None,
                    "response_headers": {},
                    "request_payload": payload,
                    "retry_count": attempt,
                    "duration_ms": int((time.monotonic() - started) * 1000),
                }
            except requests.exceptions.ConnectionError:
                if attempt < retries:
                    attempt += 1
                    time.sleep(min(2**attempt, 5))
                    continue
                return {
                    "success": False,
                    "status_code": None,
                    "message": "Unable to reach BigCommerce. Please verify network and URL settings.",
                    "url": url,
                    "response_body": None,
                    "response_headers": {},
                    "request_payload": payload,
                    "retry_count": attempt,
                    "duration_ms": int((time.monotonic() - started) * 1000),
                }
            except requests.exceptions.RequestException as err:
                _logger.exception("BigCommerce request failed: %s %s", method, url)
                return {
                    "success": False,
                    "status_code": None,
                    "message": "Unexpected network error while contacting BigCommerce: %s" % err,
                    "url": url,
                    "response_body": None,
                    "response_headers": {},
                    "request_payload": payload,
                    "retry_count": attempt,
                    "duration_ms": int((time.monotonic() - started) * 1000),
                }

            if response.status_code in (429, 500, 502, 503, 504) and attempt < retries:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_seconds = float(retry_after)
                    except (TypeError, ValueError):
                        sleep_seconds = min(2 ** (attempt + 1), 10)
                else:
                    sleep_seconds = min(2 ** (attempt + 1), 10)
                attempt += 1
                time.sleep(max(0.2, sleep_seconds))
                continue
            break

        content_type = (response.headers.get("Content-Type") or "").lower()
        if "application/json" in content_type or "json" in content_type:
            try:
                response_body = response.json()
            except ValueError:
                response_body = response.text
        else:
            response_body = response.text

        return {
            "success": response.ok,
            "status_code": response.status_code,
            "message": self._build_error_message(response, response_body),
            "url": url,
            "response_body": response_body,
            "response_headers": dict(response.headers or {}),
            "request_payload": payload,
            "retry_count": attempt,
            "duration_ms": int((time.monotonic() - started) * 1000),
        }

    def get(self, path, params=None, timeout=None, retries=None):
        """Perform a GET request against BigCommerce."""
        return self._request("GET", path, params=params, timeout=timeout, retries=retries)

    def post(self, path, payload=None, params=None, timeout=None, retries=None):
        """Perform a POST request against BigCommerce."""
        return self._request(
            "POST",
            path,
            params=params,
            payload=payload,
            timeout=timeout,
            retries=retries,
        )

    def put(self, path, payload=None, params=None, timeout=None, retries=None):
        """Perform a PUT request against BigCommerce."""
        return self._request(
            "PUT",
            path,
            params=params,
            payload=payload,
            timeout=timeout,
            retries=retries,
        )

    def delete(self, path, payload=None, params=None, timeout=None, retries=None):
        """Perform a DELETE request against BigCommerce."""
        return self._request(
            "DELETE",
            path,
            params=params,
            payload=payload,
            timeout=timeout,
            retries=retries,
        )

    def get_paginated(self, path, params=None, limit=None, max_pages=None, data_key="data"):
        """Fetch paginated BigCommerce resources and return aggregated items."""
        items = []
        total_retry_count = 0
        total_duration_ms = 0
        for result in self.iter_paginated(
            path=path,
            params=params,
            limit=limit,
            max_pages=max_pages,
            data_key=data_key,
        ):
            total_retry_count += int(result.get("retry_count") or 0)
            total_duration_ms += int(result.get("duration_ms") or 0)
            if not result.get("success"):
                result["items"] = items
                result["retry_count"] = total_retry_count
                result["duration_ms"] = total_duration_ms
                return result

            items.extend(result.get("items", []))

        return {
            "success": True,
            "status_code": 200,
            "message": "Paginated fetch successful.",
            "url": self._build_url(path),
            "response_body": {"count": len(items)},
            "response_headers": {},
            "items": items,
            "retry_count": total_retry_count,
            "duration_ms": total_duration_ms,
        }

    def iter_paginated(self, path, params=None, limit=None, max_pages=None, data_key="data"):
        """Yield one page at a time for scalable batch processing."""
        params = dict(params or {})
        limit = int(limit or self.DEFAULT_PAGE_LIMIT)
        if limit < 1:
            limit = self.DEFAULT_PAGE_LIMIT
        if limit > 250:
            limit = 250

        page = int(params.get("page") or 1)
        pages_processed = 0
        hard_page_limit = None if max_pages in (None, False) else max(1, int(max_pages))
        previous_signature = None

        while True:
            if hard_page_limit and pages_processed >= hard_page_limit:
                break

            page_params = dict(params)
            page_params["limit"] = limit
            page_params["page"] = page

            result = self.get(path, params=page_params)
            if not result.get("success"):
                result["page"] = page
                result["items"] = []
                yield result
                return

            response_body = result.get("response_body")
            page_items = self._extract_items(response_body, data_key=data_key)
            result["page"] = page
            result["items"] = page_items
            yield result

            pages_processed += 1
            if not page_items:
                break
            # Safety guard: if an endpoint ignores `page` and keeps returning the same data,
            # stop after detecting a repeated signature.
            signature = self._build_page_signature(page_items)
            if previous_signature and signature == previous_signature:
                break
            previous_signature = signature
            if self._is_last_page(response_body, page=page, received_count=len(page_items), limit=limit):
                break
            page += 1

    def test_connection(self):
        """Test store connectivity with a safe store-info endpoint."""
        missing = []
        if not (self.instance.store_hash or "").strip():
            missing.append("Store Hash")
        if not (self.instance.api_base_url or "").strip():
            missing.append("API Base URL")
        if not (self.instance.access_token or "").strip():
            missing.append("Access Token")
        if missing:
            return {
                "success": False,
                "status_code": None,
                "message": "Connection is not ready. Missing: %s." % ", ".join(missing),
                "url": self._build_url("/v2/store"),
                "response_body": None,
            }

        result = self.get("/v2/store", timeout=20, retries=1)
        status_code = result.get("status_code")
        response_body = result.get("response_body")

        if result.get("success"):
            if isinstance(response_body, dict) and response_body.get("id"):
                return {
                    "success": True,
                    "status_code": status_code,
                    "message": "Connected to BigCommerce store successfully.",
                    "url": result.get("url"),
                    "response_body": response_body,
                }
            return {
                "success": False,
                "status_code": status_code,
                "message": "BigCommerce responded, but the store payload was not recognized.",
                "url": result.get("url"),
                "response_body": response_body,
            }

        if status_code in (401, 403):
            message = "Authentication failed. Please verify the access token."
        elif status_code == 404:
            message = "Store endpoint was not found. Check store hash and API base URL."
        elif status_code is None:
            message = result.get("message") or "Network error while connecting to BigCommerce."
        else:
            message = "BigCommerce connection test failed with HTTP %s." % status_code

        if isinstance(response_body, (dict, list)):
            body_text = json.dumps(response_body)[:300]
            message = "%s Response: %s" % (message, body_text)
        elif isinstance(response_body, str) and response_body.strip():
            message = "%s Response: %s" % (message, response_body[:300])

        return {
            "success": False,
            "status_code": status_code,
            "message": message,
            "url": result.get("url"),
            "response_body": response_body,
        }

    def _extract_items(self, response_body, data_key="data"):
        if isinstance(response_body, dict):
            if isinstance(response_body.get(data_key), list):
                return response_body.get(data_key)
            if isinstance(response_body.get("data"), list):
                return response_body.get("data")
            return []
        if isinstance(response_body, list):
            return response_body
        return []

    def _is_last_page(self, response_body, page, received_count, limit):
        if isinstance(response_body, dict):
            meta = response_body.get("meta") or {}
            pagination = meta.get("pagination") if isinstance(meta, dict) else {}
            if isinstance(pagination, dict):
                total_pages = pagination.get("total_pages")
                current_page = pagination.get("current_page")
                if total_pages and current_page and int(current_page) >= int(total_pages):
                    return True
                total = pagination.get("total")
                per_page = pagination.get("per_page") or pagination.get("count") or limit
                try:
                    per_page = int(per_page or limit)
                except (TypeError, ValueError):
                    per_page = int(limit)
                if total is not None and int(page) * max(1, per_page) >= int(total):
                    return True
        # Do not assume "received < requested" means last page when pagination metadata
        # is missing/inconsistent; some APIs cap page size silently.
        return False

    def _build_page_signature(self, page_items):
        if not page_items:
            return ()
        sample = []
        for item in page_items[:5]:
            if isinstance(item, dict):
                sample.append(item.get("id") or item.get("sku") or item.get("name") or str(item)[:40])
            else:
                sample.append(str(item)[:40])
        return (len(page_items), tuple(sample))

    def _build_error_message(self, response, response_body):
        if response.ok:
            return "OK"
        status_code = response.status_code
        message = "BigCommerce API request failed with HTTP %s." % status_code

        if isinstance(response_body, dict):
            title = response_body.get("title") or response_body.get("message")
            if title:
                message = "%s %s" % (message, str(title))
            elif response_body.get("errors"):
                message = "%s %s" % (message, json.dumps(response_body.get("errors"))[:200])
        elif isinstance(response_body, str) and response_body.strip():
            message = "%s %s" % (message, response_body[:200])

        return message
