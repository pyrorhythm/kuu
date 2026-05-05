default:
    just --list

sync:
    uv sync --all-extras --dev --refresh

publish INDEX="pypi":
    uv publish --index="{{ INDEX }}" --trusted-publishing=always

build:
    uv build -o dist/ --no-sources

lint PATH=".":
    uv run ruff check --fix-only "{{ PATH }}"

typecheck PATH="kuu":
    uv run pyrefly check "{{ PATH }}"

push-commit MSG: sync lint typecheck (test '-q' 'no')
    git add .
    git commit -m "{{ MSG }}"
    git push

bump SEMVER:
    uv version "{{ SEMVER }}"

release-git SEMVER:
    git pull
    git add .
    git commit -m "release: {{ SEMVER }}"
    git push

tag-push SEMVER:
    git tag -a "{{ SEMVER }}" -m "release: {{ SEMVER }}"
    git push origin "{{ SEMVER }}"

release SEMVER: sync lint typecheck (test '-q' 'no') (bump SEMVER) (release-git SEMVER) (tag-push SEMVER)

[arg('q', long='quiet', short='q', value='-q')]
[arg('tb', long='tb')]
test q='' tb='short' DIR="tests/" *FLAGS:
    uv run pytest {{ FLAGS }} "{{ DIR }}" {{ q }} --tb={{ tb }} -n auto

docs:
    uv run --group docs sphinx-build -b html -W --keep-going docs docs/_build/html

docs-serve PORT="8000":
    uv run --group docs sphinx-autobuild --port {{ PORT }} docs docs/_build/html

docs-clean:
    rm -rf docs/_build docs/apidocs
