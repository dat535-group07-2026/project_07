# Japanese Vocabulary and Example Lookup

DAT535 course project by Alexander Kvalvaag and Christopher Liebich Kolle.

We are building a PySpark batch pipeline that combines Japanese example sentences,
dictionary entries and kanji information. A simple web interface will let users
look up a word, see its readings and possible definitions, and find example
sentences with English translations. We may also add an Anki export using the
same prepared data.

The main work is in the pipeline: modelling the sources, connecting them,
checking data quality, testing the processing and publishing reproducible outputs.
The interface is a small demonstration of the result.

## Current status

This project is in the setup and proof-of-concept stage. As of 8 October 2026:

- The repository has been cloned to the Spark VM.
- The original source files have been downloaded outside the repository.
- Download URLs, sizes, modification dates and checksums are recorded in a manifest.
- SudachiPy has been tested on a Japanese sentence, and its dependencies are pinned.
- Token-to-JMdict matching has not been tested yet.
- The Silver and Gold processing, web interface, tests and project-specific GitHub
  Actions workflows are not implemented yet.

The sections below describe the planned pipeline, not completed features.

## Scope

The required version will support Japanese dictionary written forms and readings.
It will combine:

- **Tatoeba:** Japanese sentences, English translations and sentence tags.
- **JMdict:** written forms, readings, parts of speech and English definitions.
- **KANJIDIC2:** meanings, readings and metadata for individual kanji characters.

We will use an existing tokenizer rather than build one. Dictionary matches are
candidates, not automatic identification of the correct meaning in a sentence.
Even a unique dictionary entry may contain several senses.

Korean, JESC, audio, user accounts, translation models and automatic grammar
correction are outside the required scope. We will not assign sentence-level
JLPT ratings. Individual kanji meanings and readings do not necessarily give
the meaning or pronunciation of a whole word.

## Sources

| Source | Files used | Role |
| --- | --- | --- |
| Tatoeba | Japanese and English detailed sentences, Japanese–English links, and tags for both languages (`.tsv.bz2`) | Examples and translation relationships |
| JMdict | Japanese–English dictionary (`JMdict_e.gz`, compressed XML) | Vocabulary entries and possible definitions |
| KANJIDIC2 | `kanjidic2.xml.gz` | Character information |

The initial download contains about 53 MB of compressed files. The five Tatoeba
files total about 43 MB compressed, JMdict about 63 MB uncompressed, and
KANJIDIC2 about 16 MB uncompressed. These are measurements of our downloaded
files, not fixed dataset sizes.

The Tatoeba snapshot dated 3 October 2026 contains 248,924 Japanese sentences,
2,038,137 English sentences and 280,716 Japanese–English links. The English file
contains all English sentences in that export, not just translations of Japanese.
The dictionary files downloaded on 8 October have their own source dates.
We will profile their entry counts during development.

This data fits on one machine. We use Spark to implement partition-based
processing, relational joins and quality summaries, and to measure relevant
performance choices. We do not claim that this workload requires a cluster.

## Planned pipeline

### Bronze: preserve the downloads

Bronze retains the original compressed files without changing their contents.
A run manifest identifies the exact combination of source versions used and
records URLs, retrieval times, source modification dates, file sizes and SHA-256
checksums. The sources are published independently and need not share a date.

We plan to check for updates weekly through GitHub Actions, after Tatoeba's
Saturday export. Each export is a full snapshot, not an incremental batch to
append to an existing sentence table. Unchanged content can be reused, while
new versions remain separate. Detecting changes before downloading will depend
on available HTTP metadata, with checksums verifying downloaded content.

Failed downloads and corrupt archives must not replace a working published
version. The initial one-off download is not yet the scheduled ingestion pipeline.

### Silver: model, validate and connect the sources

We plan to store validated tables as Parquet. Their keys will include a source
snapshot or version identifier where needed to distinguish historical records.

| Table or relationship | One row represents |
| --- | --- |
| Sentences | A sentence ID within a Tatoeba snapshot |
| Translation links | A Japanese–English ID pair within a snapshot |
| Sentence tags | A sentence–tag relationship |
| Dictionary entries, forms, readings and senses | A dictionary entity or child record within a JMdict version |
| Sentence tokens | A token position within a sentence and tokenizer version |
| Token matches | A token–dictionary-entry candidate, including the matching rule |
| Kanji records and readings/meanings | A character record or child record within a KANJIDIC2 version |
| Word–kanji relationships | A distinct character occurring in a dictionary written form |

Processing will include:

1. Parse explicit schemas, cast IDs and dates, and convert Tatoeba's `\N` to null.
2. Validate required fields and references. Quarantine malformed records and
   broken links with their original values and a reason. Missing optional owners
   or dates are reported, not automatically rejected.
3. Preserve distinct translation alternatives. Aggregate tags before enriching
   pairs so several tags do not accidentally multiply the examples.
4. Parse JMdict reading and sense restrictions without creating invalid
   combinations of written forms, readings and meanings.
5. Tokenise each Japanese sentence once, retaining surface text, dictionary form,
   reading, part of speech and token positions. Reuse tokenizer instances within
   Spark partitions.
6. Match dictionary forms to candidate JMdict entries. Reading information can
   help where compatible, but a token's reading is not necessarily the reading of
   its dictionary form. Any script normalisation or fallback rule will be explicit
   and tested. Unmatched and ambiguous tokens are not automatically bad data.
7. Connect characters in written forms to KANJIDIC2 and report unmatched kanji.

Original text will remain available. If we add normalised text, token offsets
will identify which text representation they refer to. Character handling will
not assume that every kanji fits a narrow Unicode range.

Quality reports will include input/output counts, broken references, missing
metadata, translation coverage, candidate-match coverage and ambiguity. We will
separate structural validation from linguistic uncertainty. Tatoeba warning tags
are useful signals, but their presence or absence does not prove correctness.

We will reconcile accepted, rejected and explicitly deduplicated records and
check relationship keys. Snapshot comparisons will identify additions, changes
and removals. A small manually reviewed sample will help assess matching
usefulness without claiming to validate the whole corpus.

### Gold: prepare lookup outputs

Gold will package vocabulary, candidate definitions, translated examples and
kanji information for the lookup interface. We plan to use SQLite as the serving
database, so a user search does not start a Spark job.

The interface will show a limited set of examples using stated selection rules,
such as length limits and review-warning tags. A linked English translation is
not a verified translation. Kanji details will be displayed separately from word
definitions, and results will retain source references.

A new serving version will only become ready after export and validation succeed.
Failed runs will leave the previous version available. An optional Anki export
could use the same Gold data, with one target word and selected example per card.

## Testing and performance

Planned fixtures cover missing endpoints, invalid dates, duplicate pairs,
multiple tags and translations, inflected forms, dictionary restrictions and
ambiguous matches. Tests will check schemas, keys, count reconciliation,
matching rules, deterministic reruns and known lookup queries.

We will use both DataFrame operations and Spark SQL. Performance comparisons
will use the same inputs and record repeated runtimes, execution plans and
shuffle metrics where available. Broadcast joins, caching and partition counts
will be chosen based on measurements, not prescribed in advance. Coverage
thresholds will be justified after profiling rather than assumed to be 99%.

## Development and automation

Development will use small fixed samples, with separate dev and prod output
paths. Planned GitHub Actions workflows will run tests, support manual runs,
and check for weekly updates. Code changes will be reviewed through pull
requests, and production runs will use an environment approval gate. Logs,
manifests, quality reports and serving outputs will be retained as artifacts.

The runner previously registered to the lab repository does not automatically
serve this repository. Project runner access and workflows still need setup.

## Current VM setup

```text
/home/ubuntu/project_07/          Code, documentation and dependency pins
/home/ubuntu/project-data/       Downloaded datasets and future processing outputs
```

The initial Bronze download is at:

```text
/home/ubuntu/project-data/bronze/2026-10-08/20261008T091537Z-a582484d/
├── manifest.json
├── tatoeba/
│   ├── jpn_sentences_detailed.tsv.bz2
│   ├── eng_sentences_detailed.tsv.bz2
│   ├── jpn-eng_links.tsv.bz2
│   ├── jpn_tags.tsv.bz2
│   └── eng_tags.tsv.bz2
├── jmdict/JMdict_e.gz
└── kanjidic2/kanjidic2.xml.gz
```

Full downloads and generated databases do not belong in Git. Small test fixtures
will be stored in the repository. The data root will be configurable as the
pipeline is implemented.

Clone the repository with:

```bash
git clone https://github.com/dat535-group07-2026/project_07.git
cd project_07
```

On the existing Spark VM, activate the lab environment with:

```bash
source /home/ubuntu/spark-env/bin/activate
python -m pip install -r requirements.txt
```

The tested tokenizer versions are `SudachiPy==0.6.11` and
`SudachiDict-core==20260723`. Python is 3.11.15. The environment currently reports
PySpark 4.2.0, but compatibility with the VM's Java and Spark runtime still needs
verification. The dependency file currently covers only the tokenizer setup,
not a complete pipeline installation.

There is no full-pipeline command yet. The next step is a small token-to-JMdict
matching experiment before implementing the Silver tables.

## Sources and licences

- [Tatoeba downloads](https://tatoeba.org/en/downloads) and
  [attribution guidance](https://en.wiki.tatoeba.org/articles/show/faq).
  The sentence exports are supplied under CC BY 2.0 FR. We will retain source
  links and attribution notices. A current owner username is not necessarily
  the original author.
- [JMdict/EDICT overview](https://www.edrdg.org/jmdict/edict.html) and
  [KANJIDIC2 documentation](https://www.edrdg.org/kanjidic/kanjidic2_dtdh.html).
  We use JMdict rather than duplicate it with the legacy EDICT export.
- [EDRDG licence statement](https://www.edrdg.org/edrdg/licence.html).
  Its current statement specifies CC BY-SA 4.0 for the dictionary material.
  Distributed derived dictionary data will retain the applicable attribution
  and share-alike requirements. Source acknowledgements will also appear in the UI.
- Project code is licensed under [GPL v3](LICENSE). This does not replace the
  licences of the source datasets.
