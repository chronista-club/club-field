#!/usr/bin/env bash
# field のモデルをまとめて確かめる。
#
#   docs/model/check.sh
#
# 道のりの 7 通りそれぞれについて、場面の確かめ（quint test）と、
# 性質の成否（quint run）を実行し、期待と突き合わせる。
# quint run はランダムな操作列を多数試す方式で、全状態の網羅ではない。
set -u

cd "$(dirname "$0")/../.."
MODEL=docs/model/field.qnt
SAMPLES=${SAMPLES:-20000}
STEPS=${STEPS:-30}

quint() { bunx @informalsystems/quint@0.33.1 "$@"; }

failed=0

# 道具が動くかを先に確かめる（bunx の一時キャッシュが欠けると動かなくなる）
if ! quint --version >/dev/null 2>&1; then
  echo "quint が動かない。bunx の一時キャッシュが壊れている可能性がある。"
  echo "次を消してから、もう一度実行する:"
  echo "  \${TMPDIR}bunx-\$(id -u)-@informalsystems/quint@0.33.1"
  quint --version 2>&1 | grep -m1 -E "Error|error" || true
  exit 2
fi

# 期待: ok = 成り立つ / ng = 破れる（反例が見つかる）
# 列: 道のり  性質  期待
EXPECT="
dgramNaive         allInvariants      ng
dgramNaive         seenNeverGoesBack  ng
dgramNaive         quietMeansCurrent  ng
dgramNaive         backlogAtMostTwo   ng
dgramNaive         alwaysCurrent      ng
dgramDiscard       allInvariants      ng
dgramDiscard       seenNeverGoesBack  ok
dgramDiscard       quietMeansCurrent  ng
dgramDiscard       backlogAtMostTwo   ng
dgramDiscard       alwaysCurrent      ng
dgramDiscardLife   allInvariants      ok
dgramDiscardLife   seenNeverGoesBack  ok
dgramDiscardLife   quietMeansCurrent  ng
dgramDiscardLife   backlogAtMostTwo   ng
dgramDiscardLife   alwaysCurrent      ng
streamAll          allInvariants      ok
streamAll          seenNeverGoesBack  ok
streamAll          quietMeansCurrent  ok
streamAll          backlogAtMostTwo   ng
streamAll          alwaysCurrent      ng
streamLatest       allInvariants      ok
streamLatest       seenNeverGoesBack  ok
streamLatest       quietMeansCurrent  ok
streamLatest       backlogAtMostTwo   ok
streamLatest       alwaysCurrent      ng
streamLatestEcho   allInvariants      ok
streamLatestEcho   seenNeverGoesBack  ng
streamLatestEcho   quietMeansCurrent  ng
streamLatestEcho   backlogAtMostTwo   ok
streamLatestEcho   alwaysCurrent      ng
streamLatestMerge  allInvariants      ok
streamLatestMerge  seenNeverGoesBack  ok
streamLatestMerge  quietMeansCurrent  ok
streamLatestMerge  backlogAtMostTwo   ok
streamLatestMerge  alwaysCurrent      ng
"

echo "== 場面の確かめ =="
MODULES="dgramNaive dgramDiscard dgramDiscardLife streamAll streamLatest streamLatestEcho streamLatestMerge"
for main in $MODULES; do
  out=$(quint test "$MODEL" --main="$main" 2>&1)
  summary=$(echo "$out" | grep -E "passing|failing" | tr '\n' ' ')
  # 「passing があり、failing が無い」ときだけ OK。出力が無いのは失敗として扱う
  if echo "$out" | grep -q "passing" && ! echo "$out" | grep -q "failing"; then
    echo "OK  $main  $summary"
  else
    failed=1
    echo "NG  $main  $summary"
    echo "$out" | tail -20
  fi
done

echo
echo "== 性質の成否（${SAMPLES} 本 × ${STEPS} 手）=="
printf '%-4s %-18s %-18s %-6s %s\n' "" "道のり" "性質" "期待" "結果"
while read -r main inv expect; do
  [ -z "$main" ] && continue
  out=$(quint run "$MODEL" --main="$main" --invariant="$inv" \
    --max-samples="$SAMPLES" --max-steps="$STEPS" 2>&1)
  if echo "$out" | grep -q "No violation found"; then
    actual=ok
  elif echo "$out" | grep -q "Invariant violated"; then
    actual=ng
  else
    actual=error
  fi
  if [ "$actual" = "$expect" ]; then mark=OK; else mark=NG; failed=1; fi
  printf '%-4s %-18s %-18s %-6s %s\n' "$mark" "$main" "$inv" "$expect" "$actual"
  [ "$actual" = "error" ] && echo "$out" | tail -8
done <<< "$EXPECT"

echo
if [ "$failed" = 0 ]; then
  echo "期待どおり"
else
  echo "期待と違う結果がある"
  exit 1
fi
