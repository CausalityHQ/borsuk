"""SQ8-targeted scalar correction core for the V113 resident nominee screen.

This module works on bounded row batches. It neither uses query/GT data to
encode rows nor claims that the residual PQ codebook is already trained.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True, slots=True)
class Corrections:
    nu_scale: float
    alpha_scale: float
    nu_codes: np.ndarray
    alpha_codes: np.ndarray
    residual: np.ndarray
    residual_cast_error_max: float = 0.0


def _scale_and_encode(values: np.ndarray) -> tuple[float, np.ndarray]:
    bound = float(np.max(np.abs(values)))
    scale = bound / 32767.0 if bound else 1.0
    rounded = np.rint(values / scale)
    if not np.isfinite(scale) or (np.abs(rounded) > 32767).any():
        raise ValueError("scalar correction outside signed 16-bit range")
    return scale, rounded.astype(np.int16)


def encode_corrections(
    pq_reconstruction: np.ndarray,
    sq8_reconstruction: np.ndarray,
    sq8_norm: np.ndarray,
) -> Corrections:
    """Fit full-generation scalar scales and encode SQ8-targeted residuals.

    The caller must pass the complete generation to fit the scales. Batches
    appended later are checked against these scales by the writer, not here.
    """
    pq = np.asarray(pq_reconstruction)
    sq8 = np.asarray(sq8_reconstruction)
    norm = np.asarray(sq8_norm)
    if (
        pq.ndim != 2
        or pq.dtype != np.float32
        or pq.shape[1] == 0
        or sq8.shape != pq.shape
        or sq8.dtype not in (np.float32, np.float64)
        or norm.shape != (pq.shape[0],)
        or norm.dtype != np.float32
        or pq.shape[0] == 0
        or not np.isfinite(pq).all()
        or not np.isfinite(sq8).all()
        or not np.isfinite(norm).all()
    ):
        raise ValueError("SQ8/PQ correction inputs differ")
    p = pq.astype(np.float64)
    target = sq8.astype(np.float64)
    error = target - p
    squared = np.sum(p * p, axis=1)
    nu = norm.astype(np.float64) - squared
    alpha = np.divide(
        np.sum(error * p, axis=1), squared,
        out=np.zeros_like(squared), where=squared > 0,
    )
    # A tiny nonzero PQ norm can create an enormous ratio. Clipping alpha is
    # exact for this representation because the residual target uses alpha_hat.
    alpha = np.clip(alpha, -1.0, 1.0)
    nu_scale, nu_codes = _scale_and_encode(nu)
    alpha_scale = 1.0 / 32767.0
    alpha_codes = np.rint(alpha / alpha_scale).astype(np.int16)
    alpha_hat = alpha_codes.astype(np.float64) * alpha_scale
    exact_residual = error - alpha_hat[:, None] * p
    residual = np.ascontiguousarray(exact_residual, dtype=np.float32)
    if not np.isfinite(residual).all():
        raise ValueError("nonfinite SQ8-targeted residual")
    cast_difference = exact_residual - residual.astype(np.float64)
    cast_errors = np.sqrt(np.sum(cast_difference * cast_difference, axis=1))
    cast_error_max = float(np.nextafter(cast_errors.max(), np.inf))
    return Corrections(
        nu_scale, alpha_scale, nu_codes, alpha_codes, residual,
        cast_error_max,
    )


def encode_corrections_from_codes(
    pq_books: np.ndarray,
    pq_codes: np.ndarray,
    sq8_codes: np.ndarray,
    sq8_norm: np.ndarray,
    low: np.ndarray,
    span_step: np.ndarray,
) -> Corrections:
    """Build corrections from the exact generation-bound PQ and SQ8 codes.

    The SQ8 mathematical target is dequantized in float64; comparison with
    V112's float32 scorer must include its measured accumulation allowance.
    """
    code = np.asarray(sq8_codes)
    norm = np.asarray(sq8_norm)
    origin = np.asarray(low)
    step = np.asarray(span_step)
    if (
        code.ndim != 2
        or code.dtype != np.uint8
        or norm.shape != (code.shape[0],)
        or norm.dtype != np.float32
        or origin.shape != (code.shape[1],)
        or origin.dtype != np.float32
        or step.shape != origin.shape
        or step.dtype != np.float32
        or not np.isfinite(origin).all()
        or not np.isfinite(step).all()
        or (step <= 0).any()
    ):
        raise ValueError("generation SQ8 code or scale differs")
    pq = _decode_pq(np.asarray(pq_books), np.asarray(pq_codes), code.shape[1], 64)
    target = origin.astype(np.float64) + code.astype(np.float64) * step.astype(np.float64)
    return encode_corrections(pq, target, norm)


def score_nominees(
    query: np.ndarray,
    pq_reconstruction: np.ndarray,
    corrections: Corrections,
    residual_reconstruction: np.ndarray,
) -> np.ndarray:
    """Score one nominee batch, using a reconstructed residual PQ vector."""
    q = np.asarray(query)
    pq = np.asarray(pq_reconstruction)
    residual = np.asarray(residual_reconstruction)
    if (
        q.dtype != np.float32
        or q.ndim != 1
        or pq.dtype != np.float32
        or pq.ndim != 2
        or pq.shape[1] != q.size
        or residual.shape != pq.shape
        or residual.dtype != np.float32
        or corrections.nu_codes.shape != (pq.shape[0],)
        or corrections.nu_codes.dtype != np.int16
        or corrections.alpha_codes.shape != (pq.shape[0],)
        or corrections.alpha_codes.dtype != np.int16
        or not np.isfinite(corrections.nu_scale)
        or corrections.nu_scale <= 0
        or not np.isfinite(corrections.alpha_scale)
        or corrections.alpha_scale <= 0
        or not np.isfinite(q).all()
        or not np.isfinite(pq).all()
        or not np.isfinite(residual).all()
    ):
        raise ValueError("resident nominee score inputs differ")
    q64 = q.astype(np.float64)
    p = pq.astype(np.float64)
    r = residual.astype(np.float64)
    pnorm = np.sum(p * p, axis=1)
    nu = corrections.nu_codes.astype(np.float64) * corrections.nu_scale
    alpha = corrections.alpha_codes.astype(np.float64) * corrections.alpha_scale
    scores = (
        q64 @ q64 + pnorm + nu
        - 2.0 * ((1.0 + alpha) * (p @ q64) + r @ q64)
    )
    if not np.isfinite(scores).all():
        raise ValueError("nonfinite resident nominee score")
    return scores


def fit_residual_pq(
    residual: np.ndarray, *, seed: int, sample_rows: int, iterations: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Fit/encode a 12-byte PQ and record full-corpus centroid error maxima.

    This is an offline research builder. Its input is corpus-derived residuals
    only. A production builder must stream the generation into fixed memory.
    """
    from scripts.v97_row_width_screen import PqSpec, encode_pq, fit_pq

    values = np.asarray(residual)
    if (
        values.ndim != 2
        or values.dtype != np.float32
        or values.shape[0] < 256
        or values.shape[1] < 12
        or not np.isfinite(values).all()
    ):
        raise ValueError("residual PQ training rows differ")
    padded = _partition_padded(values, 12)
    specification = PqSpec("v113-r16a-residual-pq12", 12, 8, 12)
    books = fit_pq(
        padded, specification, seed=seed,
        sample_rows=sample_rows, iterations=iterations,
    )
    codes = encode_pq(padded, books, specification)
    decoded = np.empty_like(padded)
    width = padded.shape[1] // 12
    maximum_error = np.zeros((12, 256), dtype=np.float64)
    for subspace in range(12):
        lo = subspace * width
        hi = lo + width
        decoded[:, lo:hi] = books[subspace, codes[:, subspace]]
        difference = padded[:, lo:hi].astype(np.float64) - decoded[:, lo:hi]
        error = np.sqrt(np.sum(difference * difference, axis=1))
        np.maximum.at(maximum_error[subspace], codes[:, subspace], error)
    maximum_error = np.nextafter(maximum_error, np.inf)
    return books, codes, _unpartition(decoded, values.shape[1], 12), maximum_error


def _partition_padded(values: np.ndarray, subspaces: int) -> np.ndarray:
    """Balance active coordinates across every byte, then pad each block."""
    rows, dimensions = values.shape
    if dimensions < subspaces:
        raise ValueError("dimensions smaller than resident PQ subspaces")
    width = (dimensions + subspaces - 1) // subspaces
    packed = np.zeros((rows, subspaces * width), dtype=np.float32)
    for subspace in range(subspaces):
        lo = subspace * dimensions // subspaces
        hi = (subspace + 1) * dimensions // subspaces
        packed[:, subspace * width:subspace * width + hi - lo] = values[:, lo:hi]
    return packed


def _unpartition(packed: np.ndarray, dimensions: int, subspaces: int) -> np.ndarray:
    width = packed.shape[1] // subspaces
    result = np.empty((packed.shape[0], dimensions), dtype=np.float32)
    for subspace in range(subspaces):
        lo = subspace * dimensions // subspaces
        hi = (subspace + 1) * dimensions // subspaces
        result[:, lo:hi] = packed[:, subspace * width:subspace * width + hi - lo]
    return result


def _decode_pq(
    books: np.ndarray, codes: np.ndarray, dimensions: int,
    subspaces: int,
) -> np.ndarray:
    if (
        books.dtype != np.float32
        or books.ndim != 3
        or books.shape[0] != subspaces
        or books.shape[1] != 256
        or codes.dtype != np.uint8
        or codes.ndim != 2
        or codes.shape[1] != subspaces
        or dimensions < subspaces
        or books.shape[2] * subspaces !=
            ((dimensions + subspaces - 1) // subspaces) * subspaces
        or not np.isfinite(books).all()
    ):
        raise ValueError("resident PQ codebook or code shape differs")
    width = books.shape[2]
    decoded = np.empty((codes.shape[0], subspaces * width), dtype=np.float32)
    for subspace in range(subspaces):
        decoded[:, subspace * width:(subspace + 1) * width] = (
            books[subspace, codes[:, subspace]]
        )
    return _unpartition(decoded, dimensions, subspaces)


def score_encoded_nominees(
    query: np.ndarray,
    pq_books: np.ndarray,
    pq_codes: np.ndarray,
    residual_books: np.ndarray,
    residual_codes: np.ndarray,
    nu_scale: float,
    nu_codes: np.ndarray,
    alpha_scale: float,
    alpha_codes: np.ndarray,
) -> np.ndarray:
    """Score nominees from resident code planes, without SQ8 page access."""
    q = np.asarray(query)
    pq_code = np.asarray(pq_codes)
    residual_code = np.asarray(residual_codes)
    nu_code = np.asarray(nu_codes)
    alpha_code = np.asarray(alpha_codes)
    if (
        q.ndim != 1 or q.dtype != np.float32
        or not np.isfinite(q).all()
        or pq_code.ndim != 2 or pq_code.dtype != np.uint8
        or residual_code.ndim != 2 or residual_code.dtype != np.uint8
        or pq_code.shape[0] != residual_code.shape[0]
        or not isinstance(nu_scale, (int, float, np.integer, np.floating))
        or not np.isfinite(nu_scale) or nu_scale <= 0
        or not isinstance(alpha_scale, (int, float, np.integer, np.floating))
        or not np.isfinite(alpha_scale) or alpha_scale <= 0
        or nu_code.dtype != np.int16
        or alpha_code.dtype != np.int16
        or nu_code.shape != (pq_code.shape[0],)
        or alpha_code.shape != (pq_code.shape[0],)
    ):
        raise ValueError("resident nominee encoded inputs differ")
    pq = _decode_pq(np.asarray(pq_books), pq_code, q.size, 64)
    residual = _decode_pq(np.asarray(residual_books), residual_code, q.size, 12)
    corrections = Corrections(
        float(nu_scale), float(alpha_scale), nu_code,
        alpha_code, residual,
    )
    return score_nominees(q, pq, corrections, residual)
