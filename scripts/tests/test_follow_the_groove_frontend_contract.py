from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from scripts.follow_the_groove import ranking as python_ranking


ROOT = Path(__file__).resolve().parents[2]


def run_typescript(module: str, expression: str, payload: object) -> object:
    module_path = ROOT / module
    temporary_module: Path | None = None
    if module.endswith("representative-cover.ts") or module.endswith("destination-selection.ts"):
        source = module_path.read_text()
        if module.endswith("representative-cover.ts"):
            source = source.replace(
                'from "../cover-url"',
                f'from "{(ROOT / "lib/cover-url.ts").as_uri()}"',
            )
        else:
            source = source.replace(
                'from "./ranking"',
                f'from "{(ROOT / "lib/follow-the-groove/ranking.ts").as_uri()}"',
            )
        handle = tempfile.NamedTemporaryFile(mode="w", suffix=".ts", delete=False)
        with handle:
            handle.write(source)
        temporary_module = Path(handle.name)
        module_path = temporary_module
    script = f"""
import * as subject from {json.dumps(module_path.as_uri())};
const chunks = [];
for await (const chunk of process.stdin) chunks.push(chunk);
const input = JSON.parse(Buffer.concat(chunks).toString("utf8"));
const result = await ({expression});
process.stdout.write(JSON.stringify(result));
"""
    try:
        completed = subprocess.run(
            ["node", "--experimental-strip-types", "--input-type=module", "-e", script],
            cwd=ROOT,
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            check=True,
        )
        return json.loads(completed.stdout)
    finally:
        if temporary_module:
            temporary_module.unlink(missing_ok=True)


def fixture_candidate(
    name: str,
    *,
    factual: bool = False,
    mechanisms: tuple[str, ...] = (),
    similarity: bool = False,
    position: int | None = None,
    eligible: bool = True,
    product_count: int = 1,
    output_status: str = "proven_output",
) -> dict[str, object]:
    key = name.lower().replace(" ", "-")
    return {
        "sourceArtistId": "source",
        "targetArtistId": key,
        "displayName": name,
        "musicbrainzArtistMbid": f"{key}-mbid",
        "entityType": "person",
        "factual": factual,
        "factualMechanisms": list(mechanisms),
        "allowedEvidenceCount": 1 if factual else 0,
        "uniqueRecordingCount": 1 if set(mechanisms) & {"artist_credit", "instrument", "vocal"} else 0,
        "similarity": similarity,
        "similarityPosition": position,
        "similarityMatchScore": 0.5 if similarity else None,
        "searchEligible": eligible,
        "productCount": product_count,
        "destinationOutputStatus": output_status,
    }


def to_python_candidate(row: dict[str, object]) -> python_ranking.Candidate:
    return python_ranking.Candidate(
        source_artist_id=str(row["sourceArtistId"]),
        target_artist_id=str(row["targetArtistId"]),
        display_name=str(row["displayName"]),
        musicbrainz_artist_mbid=str(row["musicbrainzArtistMbid"]),
        entity_type=str(row["entityType"]),
        factual=bool(row["factual"]),
        factual_mechanisms=tuple(row["factualMechanisms"]),
        allowed_evidence_count=int(row["allowedEvidenceCount"]),
        unique_recording_count=int(row["uniqueRecordingCount"]),
        similarity=bool(row["similarity"]),
        similarity_position=row["similarityPosition"],
        similarity_match_score=None,
        search_eligible=bool(row["searchEligible"]),
        product_count=int(row["productCount"]),
    )


class V3TypescriptParityTests(unittest.TestCase):
    def test_rank_and_reason_parity_with_python_authority(self) -> None:
        rows = [
            fixture_candidate("Multi", factual=True, mechanisms=("membership",), similarity=True, position=4),
            fixture_candidate("Factual B", factual=True, mechanisms=("artist_credit",), product_count=99),
            fixture_candidate("Factual A", factual=True, mechanisms=("membership",)),
            fixture_candidate("Discovery", similarity=True, position=1),
            fixture_candidate("Later", similarity=True, position=2, eligible=False),
        ]
        for mode in ("trail", "search"):
            for limit in range(0, 6):
                with self.subTest(mode=mode, limit=limit):
                    expected = python_ranking.rank_candidates(
                        [to_python_candidate(row) for row in rows], mode=mode, limit=limit
                    )
                    actual = run_typescript(
                        "lib/follow-the-groove/ranking.ts",
                        "subject.rankCandidates(input.rows, input.options).map(row => ({ id: row.targetArtistId, reasons: subject.getReasonCodes(row) }))",
                        {"rows": rows, "options": {"mode": mode, "limit": limit}},
                    )
                    self.assertEqual(
                        actual,
                        [{"id": row.target_artist_id, "reasons": list(row.reason_codes)} for row in expected],
                    )

    def test_product_count_does_not_affect_order(self) -> None:
        rows = [
            fixture_candidate("B", factual=True, mechanisms=("membership",), product_count=999),
            fixture_candidate("A", factual=True, mechanisms=("membership",), product_count=0),
        ]
        actual = run_typescript(
            "lib/follow-the-groove/ranking.ts",
            "subject.rankCandidates(input, { mode: 'trail', limit: 5 }).map(row => row.displayName)",
            rows,
        )
        self.assertEqual(actual, ["A", "B"])

    def test_search_eligibility_filters_before_final_three_without_reranking(self) -> None:
        rows = [
            fixture_candidate("Strong zero", factual=True, product_count=0),
            fixture_candidate("Eligible two", factual=True, product_count=2),
            fixture_candidate("Weak zero", factual=True, product_count=0),
            fixture_candidate("Eligible four", factual=True, product_count=4),
            fixture_candidate("Eligible five", factual=True, product_count=5),
        ]
        for row in rows:
            row["searchEligible"] = int(row["productCount"]) > 0
        actual = run_typescript(
            "lib/follow-the-groove/ranking.ts",
            "subject.rankCandidates(input, { mode: 'search', limit: 24 }).map(row => row.displayName).filter(name => name.startsWith('Eligible')) .slice(0, 3)",
            rows,
        )
        self.assertEqual(actual, ["Eligible five", "Eligible four", "Eligible two"])

    def test_trail_mode_keeps_zero_product_candidates(self) -> None:
        row = fixture_candidate("Discovery only", factual=True, product_count=0)
        row["searchEligible"] = False
        actual = run_typescript(
            "lib/follow-the-groove/ranking.ts",
            "subject.rankCandidates(input, { mode: 'trail', limit: 5 }).map(row => row.displayName)",
            [row],
        )
        self.assertEqual(actual, ["Discovery only"])


class NextDestinationSelectionTests(unittest.TestCase):
    def select_family_destinations(
        self, source_name: str, direct: list[dict[str, object]], limit: int = 5
    ) -> list[str]:
        return run_typescript(
            "lib/follow-the-groove/destination-selection.ts",
            "subject.selectNextDestinations({ sourceArtistId: 'source', sourceArtistName: input.sourceName, direct: input.direct, onward: new Map(), limit: input.limit }).map(row => row.displayName)",
            {"sourceName": source_name, "direct": direct, "limit": limit},
        )

    def test_active_ella_ensemble_variants_are_suppressed(self) -> None:
        direct = [
            fixture_candidate("Ella Fitzgerald and Her Famous Orchestra", factual=True),
            fixture_candidate("Ella Fitzgerald and Her Savoy Eight", factual=True),
            fixture_candidate("Louis Armstrong", factual=True),
        ]
        self.assertEqual(
            self.select_family_destinations("Ella Fitzgerald", direct),
            ["Louis Armstrong"],
        )

    def test_canonical_candidate_wins_but_lone_ensemble_variant_remains(self) -> None:
        variant = fixture_candidate("Billie Holiday and Her Orchestra", factual=True)
        canonical = fixture_candidate("Billie Holiday", factual=True)
        self.assertEqual(
            self.select_family_destinations("Ella Fitzgerald", [variant, canonical]),
            ["Billie Holiday"],
        )
        self.assertEqual(
            self.select_family_destinations("Ella Fitzgerald", [variant]),
            ["Billie Holiday and Her Orchestra"],
        )

    def test_named_band_and_collaboration_are_not_family_variants(self) -> None:
        direct = [
            fixture_candidate("Tom Petty", factual=True),
            fixture_candidate("Tom Petty and the Heartbreakers", factual=True),
            fixture_candidate("Ella Fitzgerald & Louis Armstrong", factual=True),
        ]
        self.assertEqual(
            self.select_family_destinations("Ella Fitzgerald", direct),
            [
                "Ella Fitzgerald & Louis Armstrong",
                "Tom Petty",
                "Tom Petty and the Heartbreakers",
            ],
        )

    def test_family_suppression_backfills_without_reordering_survivors(self) -> None:
        names = [
            "Billie Holiday",
            "Billie Holiday and Her Orchestra",
            "Ella Fitzgerald and Her Famous Orchestra",
            "Louis Armstrong",
            "Sarah Vaughan",
            "Duke Ellington",
            "Count Basie",
        ]
        direct = [
            fixture_candidate(name, similarity=True, position=index)
            for index, name in enumerate(names, start=1)
        ]
        self.assertEqual(
            self.select_family_destinations("Ella Fitzgerald", direct),
            ["Billie Holiday", "Louis Armstrong", "Sarah Vaughan", "Duke Ellington", "Count Basie"],
        )

    def test_output_status_gates_direct_destinations_and_fills(self) -> None:
        direct = [
            fixture_candidate("Eligible A", factual=True),
            fixture_candidate("Unknown", factual=True, output_status="unknown"),
            fixture_candidate("Bridge Only", factual=True, output_status="proven_bridge_only"),
            fixture_candidate("Eligible B", factual=True),
            fixture_candidate("Eligible C", factual=True),
        ]
        actual = run_typescript(
            "lib/follow-the-groove/destination-selection.ts",
            "subject.selectNextDestinations({ sourceArtistId: 'source', direct: input, onward: new Map(), limit: 3 }).map(row => row.displayName)",
            direct,
        )
        self.assertEqual(actual, ["Eligible A", "Eligible B", "Eligible C"])

    def test_unknown_bridge_is_hidden_but_proven_child_and_winning_path_survive(self) -> None:
        bridge = fixture_candidate("Unknown Bridge", factual=True, output_status="unknown")
        child = {**fixture_candidate("Proven Child", factual=True), "sourceArtistId": bridge["targetArtistId"]}
        actual = run_typescript(
            "lib/follow-the-groove/destination-selection.ts",
            "subject.selectNextDestinations({ sourceArtistId: 'source', direct: [input.bridge], onward: new Map([[input.bridge.targetArtistId, [input.child]]]), limit: 5 }).map(row => ({ name: row.displayName, reason: row.bridgeName ? `Via ${row.bridgeName}` : null }))",
            {"bridge": bridge, "child": child},
        )
        self.assertEqual(actual, [{"name": "Proven Child", "reason": "Via Unknown Bridge"}])

    def test_zero_product_proven_output_is_standalone_only(self) -> None:
        row = fixture_candidate("Zero Product", factual=True, product_count=0, eligible=False)
        standalone = run_typescript(
            "lib/follow-the-groove/destination-selection.ts",
            "subject.selectNextDestinations({ sourceArtistId: 'source', direct: [input], onward: new Map(), limit: 5 }).map(row => row.displayName)",
            row,
        )
        search = run_typescript(
            "lib/follow-the-groove/destination-selection.ts",
            "subject.selectNextDestinations({ sourceArtistId: 'source', direct: [input], onward: new Map(), requireSearchEligible: true, limit: 3 }).map(row => row.displayName)",
            row,
        )
        self.assertEqual(standalone, ["Zero Product"])
        self.assertEqual(search, [])

    def test_search_requires_proven_output_even_with_products_and_href_eligibility(self) -> None:
        unknown = fixture_candidate("Unknown With Products", factual=True, eligible=True, product_count=10, output_status="unknown")
        actual = run_typescript(
            "lib/follow-the-groove/destination-selection.ts",
            "subject.selectNextDestinations({ sourceArtistId: 'source', direct: [input], onward: new Map(), requireSearchEligible: true, limit: 3 }).map(row => row.displayName)",
            unknown,
        )
        self.assertEqual(actual, [])

    def test_search_selector_excludes_zero_product_direct_and_indirect_candidates(self) -> None:
        direct = [
            {**fixture_candidate("Zero Direct", factual=True, product_count=0), "searchEligible": False},
            fixture_candidate("Eligible Direct", factual=True),
        ]
        onward = {
            "eligible-direct": [
                {**fixture_candidate("Zero Indirect", factual=True, product_count=0), "sourceArtistId": "eligible-direct", "searchEligible": False},
                {**fixture_candidate("Eligible Four", factual=True), "sourceArtistId": "eligible-direct"},
            ]
        }
        actual = run_typescript(
            "lib/follow-the-groove/destination-selection.ts",
            "subject.selectNextDestinations({ sourceArtistId: 'source', direct: input.direct, onward: new Map(Object.entries(input.onward)), requireSearchEligible: true, limit: 3 }).map(row => row.displayName)",
            {"direct": direct, "onward": onward},
        )
        self.assertEqual(actual, ["Eligible Direct", "Eligible Four"])

    def test_search_selector_allows_eligible_child_through_zero_product_bridge(self) -> None:
        bridge = {**fixture_candidate("Zero Product Bridge", factual=True, product_count=0), "searchEligible": False}
        child = {**fixture_candidate("Eligible Child", factual=True), "sourceArtistId": bridge["targetArtistId"]}
        actual = run_typescript(
            "lib/follow-the-groove/destination-selection.ts",
            "subject.selectNextDestinations({ sourceArtistId: 'source', direct: [input.bridge], onward: new Map([[input.bridge.targetArtistId, [input.child]]]), requireSearchEligible: true, limit: 3 }).map(row => row.displayName)",
            {"bridge": bridge, "child": child},
        )
        self.assertEqual(actual, ["Eligible Child"])

    def test_search_selector_fills_after_ineligible_candidate_without_product_bias(self) -> None:
        direct = [
            {**fixture_candidate("Eligible A", factual=True), "searchEligible": True},
            {**fixture_candidate("Zero C", factual=True, product_count=0), "searchEligible": False},
            {**fixture_candidate("Eligible B", factual=True), "searchEligible": True},
            {**fixture_candidate("Eligible D", factual=True), "searchEligible": True},
        ]
        actual = run_typescript(
            "lib/follow-the-groove/destination-selection.ts",
            "subject.selectNextDestinations({ sourceArtistId: 'source', direct: input.direct, onward: new Map(), requireSearchEligible: true, limit: 3 }).map(row => row.displayName)",
            {"direct": direct},
        )
        self.assertEqual(actual, ["Eligible A", "Eligible B", "Eligible D"])

    def test_meaningful_bridge_precedes_multiple_children(self) -> None:
        bridge = fixture_candidate("Them Crooked Vultures", factual=True, mechanisms=("artist_credit",))
        children = [
            {**fixture_candidate("John Paul Jones", factual=True, similarity=True, position=1), "sourceArtistId": "bridge"},
            {**fixture_candidate("Josh Homme", factual=True, similarity=True, position=2), "sourceArtistId": "bridge"},
        ]
        actual = run_typescript(
            "lib/follow-the-groove/destination-selection.ts",
            "subject.selectNextDestinations({ sourceArtistId: 'source', direct: [input.bridge], onward: new Map([['them-crooked-vultures', input.children]]), limit: 3 }).map(row => ({ name: row.displayName, bridge: row.bridgeName }))",
            {"bridge": bridge, "children": children},
        )
        self.assertEqual(actual[0], {"name": "Them Crooked Vultures", "bridge": None})
        self.assertEqual(actual[1]["bridge"], "Them Crooked Vultures")

    def test_bridge_semantics_apply_to_people_and_groups_without_entity_quota(self) -> None:
        for bridge_name, entity_type in (("Dave Grohl", "person"), ("Them Crooked Vultures", "group")):
            bridge = fixture_candidate(bridge_name, factual=True, mechanisms=("membership",))
            bridge["entityType"] = entity_type
            child = {**fixture_candidate("Destination", factual=True), "sourceArtistId": bridge["targetArtistId"]}
            actual = run_typescript(
                "lib/follow-the-groove/destination-selection.ts",
                "subject.selectNextDestinations({ sourceArtistId: 'source', direct: [input.bridge], onward: new Map([[input.bridge.targetArtistId, [input.child]]]), limit: 2 }).map(row => row.displayName)",
                {"bridge": bridge, "child": child},
            )
            self.assertEqual(actual, [bridge_name, "Destination"])

    def test_weak_bridge_can_be_skipped_for_stronger_child(self) -> None:
        bridge = fixture_candidate("Weak Similarity Bridge", similarity=True, position=4)
        child = {**fixture_candidate("Strong Destination", factual=True), "sourceArtistId": "weak-similarity-bridge"}
        actual = run_typescript(
            "lib/follow-the-groove/destination-selection.ts",
            "subject.selectNextDestinations({ sourceArtistId: 'source', direct: [input.bridge], onward: new Map([['weak-similarity-bridge', [input.child]]]), limit: 2 }).map(row => row.displayName)",
            {"bridge": bridge, "child": child},
        )
        self.assertEqual(actual, ["Strong Destination", "Weak Similarity Bridge"])

    def test_trail_exclusion_limit_and_input_order_are_deterministic(self) -> None:
        direct = [fixture_candidate("B", factual=True), fixture_candidate("A", factual=True)]
        expression = "subject.selectNextDestinations({ sourceArtistId: 'source', direct: input.direct, onward: new Map(), excludedArtistIds: new Set(['b']), limit: 5 }).map(row => row.displayName)"
        first = run_typescript("lib/follow-the-groove/destination-selection.ts", expression, {"direct": direct})
        second = run_typescript("lib/follow-the-groove/destination-selection.ts", expression, {"direct": list(reversed(direct))})
        self.assertEqual(first, ["A"])
        self.assertEqual(first, second)

    def test_weak_direct_can_be_replaced_by_bounded_bridge_destination(self) -> None:
        direct = [
            fixture_candidate("Franz Stahl", factual=True, mechanisms=("membership",), product_count=0),
            fixture_candidate("Dave Grohl", factual=True, mechanisms=("membership",), similarity=True, position=1),
        ]
        onward = {
            "dave-grohl": [
                {**fixture_candidate("Queens of the Stone Age", factual=True, mechanisms=("artist_credit",)), "sourceArtistId": "dave-grohl"},
            ]
        }
        actual = run_typescript(
            "lib/follow-the-groove/destination-selection.ts",
            "subject.selectNextDestinations({ sourceArtistId: 'source', direct: input.direct, onward: new Map(Object.entries(input.onward)), limit: 2 }).map(row => ({ name: row.displayName, bridge: row.bridgeName }))",
            {"direct": direct, "onward": onward},
        )
        self.assertIn({"name": "Queens of the Stone Age", "bridge": "Dave Grohl"}, actual)
        self.assertNotIn({"name": "Franz Stahl", "bridge": None}, actual)

    def test_direct_hub_and_people_are_not_excluded(self) -> None:
        direct = [fixture_candidate("Person Hub", factual=True), fixture_candidate("Group", factual=True)]
        onward = {
            "person-hub": [
                {**fixture_candidate("Destination A", similarity=True, position=1), "sourceArtistId": "person-hub"},
                {**fixture_candidate("Destination B", similarity=True, position=2), "sourceArtistId": "person-hub"},
            ],
            "group": [],
        }
        actual = run_typescript(
            "lib/follow-the-groove/destination-selection.ts",
            "subject.selectNextDestinations({ sourceArtistId: 'source', direct: input.direct, onward: new Map(Object.entries(input.onward)), limit: 5 }).map(row => row.displayName)",
            {"direct": direct, "onward": onward},
        )
        self.assertIn("Person Hub", actual)

    def test_selection_is_bounded_deterministic_and_collapses_duplicates(self) -> None:
        direct = [fixture_candidate("Bridge", factual=True, similarity=True, position=1)]
        relation = {**fixture_candidate("Destination", factual=True), "sourceArtistId": "bridge"}
        actual = run_typescript(
            "lib/follow-the-groove/destination-selection.ts",
            "subject.selectNextDestinations({ sourceArtistId: 'source', direct: input.direct, onward: new Map([['bridge', [input.relation, input.relation]]]), limit: 5 }).map(row => ({ name: row.displayName, count: row.onwardCount }))",
            {"direct": direct, "relation": relation},
        )
        self.assertEqual(actual.count({"name": "Destination", "count": 1}), 1)

    def test_zero_products_is_not_a_dead_end_rule_and_depth_does_not_recurse(self) -> None:
        direct = [fixture_candidate("Hub", factual=True, product_count=0)]
        onward = {
            "hub": [
                {**fixture_candidate("One Hop", factual=True), "sourceArtistId": "hub"},
            ],
            "one-hop": [
                {**fixture_candidate("Two Hop", factual=True), "sourceArtistId": "one-hop"},
            ],
        }
        actual = run_typescript(
            "lib/follow-the-groove/destination-selection.ts",
            "subject.selectNextDestinations({ sourceArtistId: 'source', direct: input.direct, onward: new Map(Object.entries(input.onward)), limit: 5 }).map(row => row.displayName)",
            {"direct": direct, "onward": onward},
        )
        self.assertIn("Hub", actual)
        self.assertIn("One Hop", actual)
        self.assertNotIn("Two Hop", actual)


class FrontendPureContractTests(unittest.TestCase):
    def test_explained_trail_reconstructs_refresh_back_forward_and_prefix(self) -> None:
        payload = {
            "artists": [
                {"mbid": "a", "name": "Source"},
                {"mbid": "b", "name": "Middle"},
                {"mbid": "c", "name": "Destination"},
            ],
            "explanations": [None, "Bandconnectie", "Via Bridge"],
        }
        expression = "({ full: subject.buildExplainedTrail(input.artists, input.explanations), refreshed: subject.buildExplainedTrail(input.artists, input.explanations), prefix: subject.trailPrefix(subject.buildExplainedTrail(input.artists, input.explanations), 1) })"
        actual = run_typescript("lib/follow-the-groove/trail.ts", expression, payload)
        self.assertEqual(actual["full"], actual["refreshed"])
        self.assertEqual([row["name"] for row in actual["full"]], ["Source", "Middle", "Destination"])
        self.assertEqual([row["explanation"] for row in actual["full"]], [None, "Bandconnectie", "Via Bridge"])
        self.assertEqual([row["explanation"] for row in actual["prefix"]], [None, "Bandconnectie"])

    def test_search_source_resolution_order_and_safety(self) -> None:
        module = "lib/follow-the-groove/search-source.ts"
        resolve = "subject.resolveSearchGrooveSourceFromMatches(input)"
        cases = [
            ({"query": "Foo", "artistFilter": "Foo Fighters", "exactArtistNames": [], "resultArtistNames": []}, "Foo Fighters"),
            ({"query": "Foo Fighters", "exactArtistNames": ["Foo Fighters"], "resultArtistNames": ["Foo Fighters", "Other Artist"]}, "Foo Fighters"),
            ({"query": " foo   fighters ", "exactArtistNames": ["FOO FIGHTERS"], "resultArtistNames": []}, "FOO FIGHTERS"),
            ({"query": "Foo", "exactArtistNames": ["Foo Fighters"], "resultArtistNames": ["Foo Fighters", "Other Artist"]}, None),
            ({"query": "The Colour and the Shape", "exactArtistNames": [], "resultArtistNames": ["Foo Fighters"]}, "Foo Fighters"),
            ({"query": "Foo Fighters", "exactArtistNames": ["Foo Fighters", "Foo Fighters"], "resultArtistNames": []}, None),
            ({"query": "anything", "exactArtistNames": [], "resultArtistNames": ["Foo Fighters", "foo fighters"]}, "Foo Fighters"),
            ({"query": "anything", "exactArtistNames": [], "resultArtistNames": ["Foo Fighters", "Pearl Jam"]}, None),
            ({"query": "anything", "exactArtistNames": [], "resultArtistNames": []}, None),
        ]
        for payload, expected in cases:
            with self.subTest(payload=payload):
                self.assertEqual(run_typescript(module, resolve, payload), expected)

    def test_mbid_trail_and_candidate_link_contract(self) -> None:
        mbids = [
            "79239441-bfd5-4981-a70c-55c3f15c1287",
            "197450cd-0124-4164-b723-3c22dd16494d",
        ]
        actual = run_typescript(
            "lib/follow-the-groove/presentation.ts",
            "({ valid: subject.isValidTrail(input), invalid: subject.isValidTrail(['bad']), href: subject.buildGrooveHref([input[0]], input[1]) })",
            mbids,
        )
        self.assertEqual(
            actual,
            {
                "valid": True,
                "invalid": False,
                "href": "/follow-the-groove/" + "/".join(mbids),
            },
        )

    def test_reason_labels_and_membership_status(self) -> None:
        common = {
            "activeArtist": {"id": "group", "name": "The Band", "entityType": "group"},
            "candidate": {"id": "person", "entityType": "person"},
        }
        cases = [
            ({**common, "reasonCode": "factual_and_similarity", "evidence": []}, "Feitelijke én muzikale connectie"),
            ({**common, "reasonCode": "similar_artist", "evidence": []}, "Muzikaal verwant"),
            ({**common, "reasonCode": "recording_collaboration", "evidence": []}, "Werkten samen op een opname"),
            ({**common, "reasonCode": "membership", "evidence": [{"sourceArtistId": "person", "targetArtistId": "group", "evidenceKind": "membership", "ended": False}]}, "Lid van The Band"),
            ({**common, "reasonCode": "membership", "evidence": [{"sourceArtistId": "person", "targetArtistId": "group", "evidenceKind": "membership", "ended": True}]}, "Voormalig lid van The Band"),
            ({**common, "reasonCode": "membership", "evidence": [{"sourceArtistId": "person", "targetArtistId": "group", "evidenceKind": "membership", "ended": None}]}, "Bandconnectie"),
        ]
        for payload, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(
                    run_typescript("lib/follow-the-groove/reasons.ts", "subject.mapReasonLabel(input)", payload),
                    expected,
                )

    def test_cover_selection_is_exact_local_and_deterministic(self) -> None:
        payload = {
            "artists": [["artist-a", "Artist A"], ["artist-b", "Artist B"]],
            "links": [
                {"artistId": "artist-a", "productId": "b", "creditPosition": 1},
                {"artistId": "artist-a", "productId": "a", "creditPosition": 1},
                {"artistId": "artist-b", "productId": "external", "creditPosition": 1},
            ],
            "products": [
                {"id": "a", "artist": "Artist A", "coverStatus": "ready", "coverStoragePath": "aa/a.webp", "coverReviewStatus": "approved", "metadataRaw": {}},
                {"id": "b", "artist": "Artist A", "coverStatus": "ready", "coverStoragePath": "bb/b.webp", "coverReviewStatus": "approved", "metadataRaw": {}},
                {"id": "external", "artist": "Artist B", "coverStatus": "ready", "coverStoragePath": "https://shop.invalid/c.jpg", "coverReviewStatus": None, "metadataRaw": {}},
            ],
        }
        actual = run_typescript(
            "lib/follow-the-groove/representative-cover.ts",
            "Object.fromEntries(subject.resolveRepresentativeCovers(new Map(input.artists), input.links, input.products, new Set()).entries())",
            payload,
        )
        self.assertEqual(actual["artist-a"]["src"], "/covers/aa/a.webp")
        self.assertFalse(actual["artist-a"]["isPlaceholder"])
        self.assertEqual(
            actual["artist-b"]["src"],
            "/placeholders/vinylofy-cover-placeholder-white2.png",
        )
        self.assertTrue(actual["artist-b"]["isPlaceholder"])

    def test_product_count_copy_singular_plural_and_zero(self) -> None:
        actual = run_typescript(
            "lib/follow-the-groove/presentation.ts",
            "[0, 1, 2].map(subject.formatProductCount)",
            None,
        )
        self.assertEqual(actual, ["Vinylofy vond 0 titels", "Vinylofy vond 1 titel", "Vinylofy vond 2 titels"])


class FrontendSourceContractTests(unittest.TestCase):
    def test_follow_the_groove_start_page_uses_artist_only_mbid_selection(self) -> None:
        page = (ROOT / "app/follow-the-groove/page.tsx").read_text()
        form = (ROOT / "components/search/search-autocomplete-form.tsx").read_text()
        route = (ROOT / "app/api/search-suggest/route.ts").read_text()
        self.assertIn("SearchAutocompleteForm", page)
        self.assertIn('suggestionMode="follow-the-groove"', page)
        self.assertIn("selectionOnly", page)
        self.assertIn('inputId="follow-the-groove-artist"', page)
        self.assertIn("Geen artiest gevonden", page)
        self.assertIn('src="/follow-the-groove/FTG.png"', page)
        self.assertIn("h-auto w-full", page)
        self.assertIn("mx-auto max-w-3xl", page)
        self.assertIn("selectionOnly", form)
        self.assertIn("if (!selectionOnly)", form)
        self.assertIn("mode === \"follow-the-groove\"", route)
        self.assertIn("isValidArtistMbid", route)
        self.assertIn("/follow-the-groove/${encodeURIComponent", route)

    def test_start_again_returns_to_ftg_entrypoint_without_ui_redesign(self) -> None:
        page = (ROOT / "app/follow-the-groove/[...trail]/page.tsx").read_text()
        self.assertIn('href="/follow-the-groove"', page)
        self.assertIn("Start opnieuw", page)

    def test_start_page_and_search_suggestions_have_no_external_api_or_admin_client(self) -> None:
        page = (ROOT / "app/follow-the-groove/page.tsx").read_text()
        route = (ROOT / "app/api/search-suggest/route.ts").read_text()
        self.assertNotIn("createSupabaseAdminClient", page)
        self.assertNotIn("fetch(", page)
        self.assertNotIn("musicbrainz.org", route.lower())
        self.assertNotIn("last.fm", route.lower())

    def test_homepage_has_one_square_ftg_card_without_global_nav_change(self) -> None:
        homepage = (ROOT / "components/home/hero-search.tsx").read_text()
        cards = (ROOT / "components/home/home-action-cards.tsx").read_text()
        self.assertEqual(cards.count('href: "/follow-the-groove"'), 1)
        self.assertIn('imageSrc: "/follow-the-groove/ftg4.png"', cards)
        self.assertIn("grid-cols-2", cards)
        self.assertIn("md:grid-cols-4", cards)
        self.assertIn("GlobalSearchBar", homepage)
        self.assertNotIn("SiteHeader", homepage)
        self.assertNotIn("Start je groove", homepage)

    def test_provided_ftg_visual_is_local_and_not_duplicated_on_homepage(self) -> None:
        asset = ROOT / "public/follow-the-groove/FTG.png"
        home_asset = ROOT / "public/follow-the-groove/ftg4.png"
        start_page = (ROOT / "app/follow-the-groove/page.tsx").read_text()
        homepage = (ROOT / "components/home/hero-search.tsx").read_text()
        self.assertTrue(asset.is_file())
        self.assertTrue(home_asset.is_file())
        self.assertIn("/follow-the-groove/FTG.png", start_page)
        self.assertNotIn("/follow-the-groove/FTG.png", homepage)
        self.assertNotIn("http://", start_page)
        self.assertNotIn("https://", start_page)

    def test_search_service_requires_availability_and_search_href_before_ranking(self) -> None:
        data = (ROOT / "lib/follow-the-groove/data.ts").read_text()
        self.assertIn("searchPresentation?.get(candidate.id)?.searchHref !== null", data)
        self.assertIn("searchPresentation?.get(target.id)?.searchHref !== null", data)
        self.assertIn("FTG_SEARCH_RANKING_POOL_LIMIT = 24", data)
        self.assertIn("requireSearchEligible: mode === \"search\"", data)

    def test_search_block_visual_source_is_unchanged(self) -> None:
        completed = subprocess.run(
            ["git", "diff", "HEAD^", "HEAD", "--", "components/follow-the-groove/groove-search-block.tsx"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertEqual(completed.stdout, "")

    def test_search_integration_inserts_one_bounded_block_after_five(self) -> None:
        page = (ROOT / "app/search/page.tsx").read_text()
        block = (ROOT / "components/follow-the-groove/groove-search-block.tsx").read_text()
        self.assertIn("visibleResults.slice(0, 5)", page)
        self.assertIn("visibleResults.slice(5)", page)
        self.assertIn("grooveData.candidates.length > 0", page)
        self.assertEqual(page.count("<GrooveSearchBlock"), 1)
        self.assertIn("limit: 3", page)
        self.assertIn('mode: "search"', page)
        self.assertIn("buildGrooveHref([activeArtistMbid], candidate.mbid)", block)

    def test_search_block_has_no_prices_or_external_apis(self) -> None:
        block = (ROOT / "components/follow-the-groove/groove-search-block.tsx").read_text()
        self.assertNotIn("price", block.lower())
        self.assertNotIn("fetch(", block)
        self.assertNotIn("Bekijk titels", block)
        self.assertIn("Volg de groove", block)
        self.assertIn("searchHref", block)

    def test_page_preserves_service_order_and_supports_empty_state(self) -> None:
        page = (ROOT / "app/follow-the-groove/[...trail]/page.tsx").read_text()
        self.assertNotIn(".sort(", page)
        self.assertIn("data.candidates.length === 0", page)
        self.assertIn("data.candidates.map", page)
        self.assertIn('limit: 5', page)

    def test_server_only_data_and_allowed_evidence_contract(self) -> None:
        data = (ROOT / "lib/follow-the-groove/data.ts").read_text()
        self.assertIn('from "@/lib/supabase/admin"', data)
        self.assertIn('.eq("classification", "allowed")', data)
        self.assertNotIn('classification", "rejected', data)
        self.assertNotIn("fetch(", data)
        self.assertNotIn("musicbrainz.org", data.lower())
        self.assertNotIn("last.fm", data.lower())
        self.assertIn('.from("artist_output_status")', data)
        self.assertIn('=== "proven_output"', data)

    def test_output_eligibility_and_explanations_are_generic_and_batched(self) -> None:
        data = (ROOT / "lib/follow-the-groove/data.ts").read_text()
        selector = (ROOT / "lib/follow-the-groove/destination-selection.ts").read_text()
        trail = (ROOT / "components/follow-the-groove/groove-trail.tsx").read_text()
        self.assertIn('destinationOutputStatus === "proven_output"', selector)
        self.assertIn('.in("artist_id", destinationArtistIds)', data)
        self.assertIn("loadTrailExplanations(orderedTrailArtists)", data)
        self.assertIn("item.explanation", trail)
        self.assertIn('"flex min-w-0 flex-1 flex-col"', trail)
        self.assertIn("self-end px-3 text-right", trail)
        combined = data + selector
        for hardcoded in ("Foo Fighters", "Queens of the Stone Age", "Dave Grohl"):
            self.assertNotIn(hardcoded, combined)

    def test_ftg_ui_has_no_prices_external_images_or_technical_reasons(self) -> None:
        files = list((ROOT / "components/follow-the-groove").glob("*.tsx"))
        files.append(ROOT / "app/follow-the-groove/[...trail]/page.tsx")
        text = "\n".join(path.read_text() for path in files)
        self.assertNotIn("price", text.lower())
        self.assertNotIn("http://", text)
        self.assertNotIn("https://", text)
        for code in ("factual_and_similarity", "recording_collaboration", "similar_artist"):
            self.assertNotIn(code, text)
        self.assertIn("focus-visible", text)
        self.assertIn("<Link", text)
        self.assertIn("prefetch={false}", text)

    def test_zero_products_hides_cta(self) -> None:
        hero = (ROOT / "components/follow-the-groove/groove-artist-hero.tsx").read_text()
        self.assertIn("artist.productCount > 0 && artist.searchHref", hero)


if __name__ == "__main__":
    unittest.main()
