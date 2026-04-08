# -*- coding: utf-8 -*-

import logging

import requests

_logger = logging.getLogger(__name__)


class GeminiChatService:
    """Small Gemini API wrapper used by the BigCommerce dashboard assistant."""

    DEFAULT_TIMEOUT = 30
    DEFAULT_MAX_TOKENS = 900
    DEFAULT_MODEL_FALLBACKS = ("gemini-2.5-flash", "gemini-2.0-flash", "gemini-1.5-flash")
    API_URL_TEMPLATE = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    LIST_MODELS_URL = "https://generativelanguage.googleapis.com/v1beta/models"

    def ask(
        self,
        api_key,
        model,
        system_prompt,
        context_text,
        user_message,
        history=None,
        timeout=None,
    ):
        """Send one chat request to Gemini and parse the best candidate text."""
        key = (api_key or "").strip()
        model_name = self._normalize_model_name((model or "").strip() or "gemini-1.5-flash")
        message = (user_message or "").strip()
        if not key:
            return {"ok": False, "error": "Gemini API key is not configured."}
        if not message:
            return {"ok": False, "error": "Message is empty."}

        payload = self._build_payload(
            system_prompt=system_prompt,
            context_text=context_text,
            user_message=message,
            history=history or [],
        )
        first_try = self._call_generate(
            api_key=key,
            model_name=model_name,
            payload=payload,
            timeout=timeout,
        )
        if first_try.get("ok"):
            return first_try

        # Auto-recover when configured model is obsolete/not enabled.
        if self._is_model_not_found(first_try):
            fallback_model = self._discover_fallback_model(
                api_key=key,
                preferred_models=[model_name] + list(self.DEFAULT_MODEL_FALLBACKS),
                timeout=timeout,
            )
            if fallback_model and fallback_model != model_name:
                retry = self._call_generate(
                    api_key=key,
                    model_name=fallback_model,
                    payload=payload,
                    timeout=timeout,
                )
                if retry.get("ok"):
                    retry["answer"] = "%s\n\n(Used fallback model: %s)" % (retry.get("answer"), fallback_model)
                    return retry
                return retry
        return first_try

    def _build_payload(self, system_prompt, context_text, user_message, history):
        """Build Gemini generateContent payload with context + limited history."""
        history = (history or [])[-10:]
        contents = []

        context_block = (context_text or "").strip()
        if context_block:
            contents.append(
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": (
                                "Business context from Odoo BigCommerce dashboard:\n"
                                "%s\n\nUse this context only when relevant."
                            )
                            % context_block
                        }
                    ],
                }
            )

        for item in history:
            role = "model" if (item.get("role") or "").lower() == "assistant" else "user"
            text = (item.get("content") or "").strip()
            if not text:
                continue
            contents.append({"role": role, "parts": [{"text": text}]})

        contents.append({"role": "user", "parts": [{"text": user_message}]})

        return {
            "system_instruction": {
                "parts": [{"text": (system_prompt or "").strip()}],
            },
            "contents": contents,
            "generationConfig": {
                "temperature": 0.25,
                "maxOutputTokens": self.DEFAULT_MAX_TOKENS,
            },
        }

    def _extract_answer(self, data):
        """Extract plain text answer from Gemini response payload."""
        candidates = data.get("candidates") if isinstance(data, dict) else []
        if not candidates:
            return ""
        content = candidates[0].get("content") if isinstance(candidates[0], dict) else {}
        parts = content.get("parts") if isinstance(content, dict) else []
        texts = []
        for part in parts or []:
            text = part.get("text") if isinstance(part, dict) else ""
            if text:
                texts.append(text.strip())
        return "\n".join([text for text in texts if text]).strip()

    def _normalize_model_name(self, model_name):
        name = (model_name or "").strip()
        if name.startswith("models/"):
            return name.split("/", 1)[1]
        return name

    def _call_generate(self, api_key, model_name, payload, timeout=None):
        """Call Gemini generateContent for one model name."""
        url = self.API_URL_TEMPLATE.format(model=self._normalize_model_name(model_name))
        try:
            response = requests.post(
                url=url,
                params={"key": api_key},
                json=payload,
                timeout=timeout or self.DEFAULT_TIMEOUT,
            )
        except requests.exceptions.Timeout:
            return {"ok": False, "error": "Gemini request timed out. Please try again.", "status_code": None}
        except requests.exceptions.RequestException as err:
            _logger.exception("Gemini request failed")
            return {"ok": False, "error": "Unable to reach Gemini: %s" % err, "status_code": None}

        if not response.ok:
            return {
                "ok": False,
                "error": self._build_http_error(response),
                "status_code": response.status_code,
            }

        try:
            data = response.json()
        except ValueError:
            return {"ok": False, "error": "Gemini returned an invalid JSON response.", "status_code": response.status_code}

        answer = self._extract_answer(data)
        if not answer:
            return {"ok": False, "error": "Gemini returned an empty answer.", "status_code": response.status_code}

        return {"ok": True, "answer": answer, "status_code": response.status_code}

    def _is_model_not_found(self, result):
        """Return True when result indicates unsupported/missing model."""
        error_text = (result.get("error") or "").lower()
        status_code = result.get("status_code")
        if status_code == 404:
            return True
        return "is not found" in error_text or "not supported for generatecontent" in error_text

    def _discover_fallback_model(self, api_key, preferred_models=None, timeout=None):
        """List available models and pick one that supports generateContent."""
        supported = self._list_supported_models(api_key=api_key, timeout=timeout)
        if not supported:
            return ""

        normalized_supported = [self._normalize_model_name(name) for name in supported]
        preferred = [self._normalize_model_name(name) for name in (preferred_models or []) if name]
        for model_name in preferred:
            if model_name in normalized_supported:
                return model_name
        return normalized_supported[0] if normalized_supported else ""

    def _list_supported_models(self, api_key, timeout=None):
        """Return model names that support generateContent."""
        try:
            response = requests.get(
                url=self.LIST_MODELS_URL,
                params={"key": api_key},
                timeout=timeout or self.DEFAULT_TIMEOUT,
            )
        except requests.exceptions.RequestException:
            _logger.exception("Gemini list models request failed")
            return []

        if not response.ok:
            return []

        try:
            payload = response.json()
        except ValueError:
            return []

        models = payload.get("models") if isinstance(payload, dict) else []
        supported = []
        for model in models or []:
            if not isinstance(model, dict):
                continue
            methods = model.get("supportedGenerationMethods") or []
            if "generateContent" not in methods:
                continue
            name = model.get("name") or ""
            if not name:
                continue
            supported.append(name)
        return supported

    def _build_http_error(self, response):
        """Build a safe and user-friendly Gemini API error message."""
        status_code = response.status_code
        base = "Gemini request failed with HTTP %s." % status_code
        try:
            payload = response.json()
        except ValueError:
            payload = {}

        if status_code in (401, 403):
            return "Gemini authentication failed. Check the API key and model access."
        if status_code == 429:
            return "Gemini rate limit reached. Please retry in a moment."

        if isinstance(payload, dict):
            error = payload.get("error") or {}
            message = error.get("message")
            if message:
                return "%s %s" % (base, message)
        body_text = (response.text or "").strip()
        if body_text:
            return "%s %s" % (base, body_text[:200])
        return base
