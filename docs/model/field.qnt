// field のモデル（U1: 自分の field を持つ）
//
// 仕様: docs/spec/01-field.md
// 確かめ方（全部まとめて）:
//   docs/model/check.sh
//
// このモデルが扱うのは「持つ」「入る」「出ていく」「tick」「止まる」「立ち上がり直す」と、
// tick の知らせが届くまでの道のり。field の中身（何が置かれるか）は U2 で足す。
//
// 道のりは切り替えられる（下の const）。組み合わせは末尾の 4 つのモジュール:
//   dgramNaive    順不同・失われる道 / 届いたもので上書き
//   dgramDiscard  順不同・失われる道 / 古い知らせは捨てる
//   streamAll     順番どおりの道   / 溜まったぶんも全部届ける
//   streamLatest  順番どおりの道   / 溜まったら古いものから送る側が捨てる
//   dgramDiscardLife  dgramDiscard に加えて、前の代の知らせも捨てる

module field {
  // ── 切り替え ────────────────────────────────────────

  // true  = 順番どおりに、欠けずに届く道（unison の stream channel に相当）
  // false = 順番が入れ替わり、失われうる道（datagram channel に相当）
  const ORDERED: bool

  // true = 受け取る側は、手元より古い番号の知らせを捨てる
  const DISCARD_OLDER: bool

  // 順番どおりの道で、1 人あてに溜めておける知らせの上限。
  // 超えたら送る側が古いものから捨てる
  const MAX_BACKLOG: int

  // true = 受け取る側は、field の前の代（立ち上がり直す前）の知らせを捨てる
  const CHECK_LIFE: bool

  // ── 型 ──────────────────────────────────────────────

  type User = str
  type FieldName = str
  type Visitor = str

  // field が住む場所。手元とクラウドは別々の field（裁定 8）
  type Place = Local | Cloud

  // field を指す鍵。「誰の」だけでは足りず「どの field か」まで要る
  type FieldKey = { owner: User, name: FieldName }

  type Field = {
    place: Place,
    // field 自身の時計。誰もいなくても進む。
    // 状態はメモリ上だけなので、立ち上がり直すと 0 に戻る
    tick: int,
    // 動いているか
    running: bool,
    // 何代めか。立ち上がり直すたびに 1 つ増える
    life: int,
  }

  // field から入っている人へ向かう途中の知らせ
  type Message = { to: Visitor, key: FieldKey, life: int, tick: int }

  // ── 状態 ────────────────────────────────────────────

  // 存在している field
  var registry: FieldKey -> Field

  // 入っている人が、いま手元に持っている tick。
  // 鍵があること = その field に入っていること
  var seen: (Visitor, FieldKey) -> int

  // 送られたが、まだ届いていない知らせ
  var inflight: Set[Message]

  // 検証用の記録: field が生まれたときの場所
  var bornAt: FieldKey -> Place

  // 検証用の記録: 入ってから今までに手元に持った tick の最大
  var highest: (Visitor, FieldKey) -> int

  // ── 検証で使う有限の世界 ────────────────────────────

  pure val USERS: Set[User] = Set("mako", "waki")
  pure val NAMES: Set[FieldName] = Set("studio", "live")
  pure val VISITORS: Set[Visitor] = Set("v1", "v2")
  pure val PLACES: Set[Place] = Set(Local, Cloud)
  pure val MAX_TICK: int = 4
  pure val MAX_LIFE: int = 3

  // ── 補助 ────────────────────────────────────────────

  pure def larger(a: int, b: int): int = if (a > b) a else b

  // 知らせの集まりの中で一番古い番号
  pure def lowest(ms: Set[Message]): int =
    ms.fold(MAX_TICK + 1, (acc, m) => if (m.tick < acc) m.tick else acc)

  def hasField(k: FieldKey): bool = registry.keys().contains(k)

  def isRunning(k: FieldKey): bool = hasField(k) and registry.get(k).running

  def isJoined(v: Visitor, k: FieldKey): bool = seen.keys().contains((v, k))

  def joined(k: FieldKey): Set[Visitor] =
    seen.keys().filter(p => p._2 == k).map(p => p._1)

  def fieldsOf(u: User): Set[FieldKey] =
    registry.keys().filter(k => k.owner == u)

  // ある人あての、まだ届いていない知らせ
  def pendingFor(v: Visitor, k: FieldKey): Set[Message] =
    inflight.filter(m => m.to == v and m.key == k)

  // ある人あてに新しい知らせを送り出した後の、その人あての知らせ
  def afterSend(v: Visitor, k: FieldKey, l: int, t: int): Set[Message] = {
    val mine = pendingFor(v, k).union(Set({ to: v, key: k, life: l, tick: t }))
    if (ORDERED and mine.size() > MAX_BACKLOG)
      mine.filter(m => m.tick != lowest(mine))
    else
      mine
  }

  // ── 操作 ────────────────────────────────────────────

  action init = all {
    registry' = Map(),
    seen' = Map(),
    inflight' = Set(),
    bornAt' = Map(),
    highest' = Map(),
  }

  // 持つ: ユーザが、場所を選んで、自分の field を作る
  action create(u: User, n: FieldName, p: Place): bool = {
    val k = { owner: u, name: n }
    all {
      not(hasField(k)),
      registry' = registry.put(k, { place: p, tick: 0, running: true, life: 1 }),
      bornAt' = bornAt.put(k, p),
      seen' = seen,
      inflight' = inflight,
      highest' = highest,
    }
  }

  // 入る: その時点の状態を受け取り、以後の tick を受け始める
  action join(v: Visitor, k: FieldKey): bool = all {
    isRunning(k),
    not(isJoined(v, k)),
    seen' = seen.put((v, k), registry.get(k).tick),
    highest' = highest.put((v, k), registry.get(k).tick),
    registry' = registry,
    inflight' = inflight,
    bornAt' = bornAt,
  }

  // 出ていく: 以後の tick を受け取らなくなる。field はそのまま動き続ける。
  // 順番どおりの道では、道ごと閉じるので、途中の知らせも消える
  action leave(v: Visitor, k: FieldKey): bool = all {
    isJoined(v, k),
    seen' = seen.keys().exclude(Set((v, k))).mapBy(p => seen.get(p)),
    highest' = highest.keys().exclude(Set((v, k))).mapBy(p => highest.get(p)),
    inflight' = if (ORDERED) inflight.exclude(pendingFor(v, k)) else inflight,
    registry' = registry,
    bornAt' = bornAt,
  }

  // tick: field の時計が 1 つ進み、入っている全員へ知らせを送り出す。
  // 届くのは後（receive）
  action tick(k: FieldKey): bool = {
    val newTick = registry.get(k).tick + 1
    val audience = joined(k)
    val untouched = inflight.filter(m => not(m.key == k and audience.contains(m.to)))
    all {
      isRunning(k),
      newTick <= MAX_TICK,
      registry' = registry.set(k, { ...registry.get(k), tick: newTick }),
      inflight' = untouched.union(
        audience.map(v => afterSend(v, k, registry.get(k).life, newTick)).flatten()),
      seen' = seen,
      bornAt' = bornAt,
      highest' = highest,
    }
  }

  // 届く: 途中の知らせが 1 つ届く。
  // 順番どおりの道では、その人あての一番古いものからしか届かない
  action receive(m: Message): bool = {
    val p = (m.to, m.key)
    val here = isJoined(m.to, m.key)
    // 入っている人が知っている代は、field の今の代と同じ
    // （field が止まると全員が外れ、入り直したときに今の代を受け取るため）
    val fromOldLife = CHECK_LIFE and here and m.life != registry.get(m.key).life
    all {
      inflight.contains(m),
      ORDERED implies (m.tick == lowest(pendingFor(m.to, m.key))),
      inflight' = inflight.exclude(Set(m)),
      // すでに出ていった人あての知らせは捨てる
      seen' =
        if (not(here) or fromOldLife) seen
        else if (DISCARD_OLDER) seen.set(p, larger(seen.get(p), m.tick))
        else seen.set(p, m.tick),
      highest' =
        if (here and not(fromOldLife)) highest.set(p, larger(highest.get(p), m.tick))
        else highest,
      registry' = registry,
      bornAt' = bornAt,
    }
  }

  // 失われる: 途中の知らせが 1 つ、届かずに消える。順番どおりの道では起きない
  action lose(m: Message): bool = all {
    not(ORDERED),
    inflight.contains(m),
    inflight' = inflight.exclude(Set(m)),
    seen' = seen,
    highest' = highest,
    registry' = registry,
    bornAt' = bornAt,
  }

  // 止まる: field のプロセスが止まる。入っていた人は全員外れる。
  // 順番どおりの道では、道ごと閉じるので、途中の知らせも消える。
  // 順不同の道では、送り出し済みの知らせは残る
  action stop(k: FieldKey): bool = all {
    isRunning(k),
    registry' = registry.set(k, { ...registry.get(k), running: false }),
    seen' = seen.keys().filter(p => p._2 != k).mapBy(p => seen.get(p)),
    highest' = highest.keys().filter(p => p._2 != k).mapBy(p => highest.get(p)),
    inflight' = if (ORDERED) inflight.filter(m => m.key != k) else inflight,
    bornAt' = bornAt,
  }

  // 立ち上がり直す: 同じ field が、同じ場所で、次の代として動き出す。
  // 時計は 0 から
  action restart(k: FieldKey): bool = all {
    hasField(k),
    not(registry.get(k).running),
    registry.get(k).life < MAX_LIFE,
    registry' = registry.set(k, {
      ...registry.get(k), running: true, tick: 0, life: registry.get(k).life + 1
    }),
    seen' = seen,
    highest' = highest,
    inflight' = inflight,
    bornAt' = bornAt,
  }

  action step = any {
    nondet u = oneOf(USERS)
    nondet n = oneOf(NAMES)
    nondet p = oneOf(PLACES)
    create(u, n, p),

    nondet v = oneOf(VISITORS)
    nondet u = oneOf(USERS)
    nondet n = oneOf(NAMES)
    join(v, { owner: u, name: n }),

    nondet v = oneOf(VISITORS)
    nondet u = oneOf(USERS)
    nondet n = oneOf(NAMES)
    leave(v, { owner: u, name: n }),

    nondet u = oneOf(USERS)
    nondet n = oneOf(NAMES)
    tick({ owner: u, name: n }),

    nondet u = oneOf(USERS)
    nondet n = oneOf(NAMES)
    stop({ owner: u, name: n }),

    nondet u = oneOf(USERS)
    nondet n = oneOf(NAMES)
    restart({ owner: u, name: n }),

    all {
      inflight != Set(),
      nondet m = oneOf(inflight)
      receive(m),
    },

    all {
      inflight != Set(),
      nondet m = oneOf(inflight)
      lose(m),
    },
  }

  // ── どの道でも成り立つべきこと ──────────────────────

  // 手元の tick が、field の時計より先に進むことはない
  val seenNeverAhead =
    seen.keys().forall(p => seen.get(p) <= registry.get(p._2).tick)

  // 途中の知らせが、未来のことを言うことはない
  // （今の代の知らせは今の時計以下。先の代の知らせは無い）
  val inflightNeverAhead =
    inflight.forall(m => and {
      hasField(m.key),
      m.life <= registry.get(m.key).life,
      (m.life == registry.get(m.key).life) implies (m.tick <= registry.get(m.key).tick),
    })

  // 止まっている field に入っている人はいない
  val stoppedFieldHasNoOne =
    seen.keys().forall(p => isRunning(p._2))

  // field は生まれた場所に住み続ける（移さない）
  val placeNeverChanges =
    registry.keys().forall(k => registry.get(k).place == bornAt.get(k))

  // 時計は戻らない
  val tickNeverNegative =
    registry.keys().forall(k => registry.get(k).tick >= 0)

  // 存在しない field に入っている人はいない
  val noOneInMissingField =
    seen.keys().forall(p => hasField(p._2))

  val allInvariants = all {
    seenNeverAhead,
    inflightNeverAhead,
    placeNeverChanges,
    tickNeverNegative,
    noOneInMissingField,
    stoppedFieldHasNoOne,
  }

  // ── 道によって成り立ったり破れたりすること ──────────
  // どの組み合わせで成り立つかは docs/model/check.sh の表が正

  // 手元の tick は戻らない（古い知らせで上書きされない）
  val seenNeverGoesBack =
    seen.keys().forall(p => seen.get(p) == highest.get(p))

  // 途中の知らせがもう無いなら、手元は field の今の状態と同じ
  val quietMeansCurrent =
    seen.keys().forall(p =>
      (pendingFor(p._1, p._2) == Set()) implies (seen.get(p) == registry.get(p._2).tick))

  // 1 人あてに溜まっている知らせは 2 つまで
  val backlogAtMostTwo =
    seen.keys().forall(p => pendingFor(p._1, p._2).size() <= 2)

  // 入っている人は、常に field の今の状態を持っている（どの道でも破れる）
  val alwaysCurrent =
    seen.keys().forall(p => seen.get(p) == registry.get(p._2).tick)

  // ── 場面ごとの確かめ（どの道でも通る）──────────────

  pure val makoStudio: FieldKey = { owner: "mako", name: "studio" }
  pure val makoLive: FieldKey = { owner: "mako", name: "live" }
  pure val wakiStudio: FieldKey = { owner: "waki", name: "studio" }

  // 1 代めの知らせ
  pure def msg(v: Visitor, k: FieldKey, t: int): Message = { to: v, key: k, life: 1, tick: t }

  // 何代めかを指定した知らせ
  pure def msgOf(v: Visitor, k: FieldKey, l: int, t: int): Message =
    { to: v, key: k, life: l, tick: t }

  // field は誰も入っていなくても動き続ける
  run tickAdvancesWithNobodyTest =
    init
      .then(create("mako", "studio", Local))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .expect(and {
        registry.get(makoStudio).tick == 3,
        joined(makoStudio) == Set(),
        inflight == Set(),
      })

  // 入った人は、その時点の状態をすぐ受け取る。以後の tick は届いたときに反映される
  run joinGetsSnapshotThenTicksTest =
    init
      .then(create("mako", "studio", Local))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .then(join("v1", makoStudio))
      .expect(seen.get(("v1", makoStudio)) == 2)
      .then(tick(makoStudio))
      .expect(seen.get(("v1", makoStudio)) == 2)
      .then(receive(msg("v1", makoStudio, 3)))
      .expect(seen.get(("v1", makoStudio)) == 3)

  // 出ていった人には知らせが送られず、field は動き続ける
  run leaveStopsDeliveryTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(leave("v1", makoStudio))
      .then(tick(makoStudio))
      .expect(and {
        registry.get(makoStudio).tick == 1,
        inflight == Set(),
      })

  // 1 人が、手元とクラウドに別々の field を持てる
  run oneUserLocalAndCloudTest =
    init
      .then(create("mako", "studio", Local))
      .then(create("mako", "live", Cloud))
      .expect(and {
        fieldsOf("mako") == Set(makoStudio, makoLive),
        registry.get(makoStudio).place == Local,
        registry.get(makoLive).place == Cloud,
      })

  // 別々の field は互いに影響しない
  run fieldsAreIndependentTest =
    init
      .then(create("mako", "studio", Local))
      .then(create("mako", "live", Cloud))
      .then(join("v1", makoLive))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .expect(and {
        registry.get(makoStudio).tick == 2,
        registry.get(makoLive).tick == 0,
        seen.get(("v1", makoLive)) == 0,
        inflight == Set(),
      })

  // 同じ名前でも、持ち主が違えば別の field
  run sameNameDifferentOwnerTest =
    init
      .then(create("mako", "studio", Local))
      .then(create("waki", "studio", Cloud))
      .then(tick(makoStudio))
      .expect(and {
        registry.get(makoStudio).tick == 1,
        registry.get(wakiStudio).tick == 0,
      })

  // 同じ持ち主が、同じ名前の field を二つは持てない
  run duplicateNameFailsTest =
    init
      .then(create("mako", "studio", Local))
      .then(create("mako", "studio", Cloud))
      .fail()

  // 入っていない人は出ていけない
  run leaveWithoutJoinFailsTest =
    init
      .then(create("mako", "studio", Local))
      .then(leave("v1", makoStudio))
      .fail()

  // 存在しない field には入れない
  run joinMissingFieldFailsTest =
    init
      .then(join("v1", makoStudio))
      .fail()
  // 止まると、入っていた人は全員外れる
  run stopDropsEveryoneTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(join("v2", makoStudio))
      .then(stop(makoStudio))
      .expect(and {
        joined(makoStudio) == Set(),
        not(isRunning(makoStudio)),
        hasField(makoStudio),
      })

  // 止まっている field には入れない
  run cannotJoinStoppedTest =
    init
      .then(create("mako", "studio", Local))
      .then(stop(makoStudio))
      .then(join("v1", makoStudio))
      .fail()

  // 止まっている field の時計は進まない
  run cannotTickStoppedTest =
    init
      .then(create("mako", "studio", Local))
      .then(stop(makoStudio))
      .then(tick(makoStudio))
      .fail()

  // 立ち上がり直しても、同じ field（同じ持ち主・名前・場所）。代が進み、時計は 0 から
  run restartKeepsIdentityTest =
    init
      .then(create("mako", "studio", Cloud))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .then(stop(makoStudio))
      .then(restart(makoStudio))
      .expect(and {
        fieldsOf("mako") == Set(makoStudio),
        registry.get(makoStudio).place == Cloud,
        registry.get(makoStudio).life == 2,
        registry.get(makoStudio).tick == 0,
        isRunning(makoStudio),
      })

  // 立ち上がり直した後に入り直すと、新しい代の状態を受け取る
  run rejoinAfterRestartTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(tick(makoStudio))
      .then(receive(msg("v1", makoStudio, 1)))
      .then(stop(makoStudio))
      .then(restart(makoStudio))
      .then(join("v1", makoStudio))
      .expect(seen.get(("v1", makoStudio)) == 0)
      .then(tick(makoStudio))
      .then(receive(msgOf("v1", makoStudio, 2, 1)))
      .expect(seen.get(("v1", makoStudio)) == 1)

  // 動いている field は立ち上がり直せない
  run cannotRestartRunningTest =
    init
      .then(create("mako", "studio", Local))
      .then(restart(makoStudio))
      .fail()
}

// ── 順不同・失われる道 / 届いたもので上書き ─────────────
module dgramNaive {
  import field(ORDERED = false, DISCARD_OLDER = false, MAX_BACKLOG = 99, CHECK_LIFE = false).*

  // 知らせが 1 つ失われても、次の知らせで追いつく
  run lostTickIsRecoveredByNextTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(tick(makoStudio))
      .then(lose(msg("v1", makoStudio, 1)))
      .expect(seen.get(("v1", makoStudio)) == 0)
      .then(tick(makoStudio))
      .then(receive(msg("v1", makoStudio, 2)))
      .expect(seen.get(("v1", makoStudio)) == 2)

  // 上書きの決まりだと、順番が入れ替わったときに手元が古い状態に戻る
  run outOfOrderGoesBackTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .then(receive(msg("v1", makoStudio, 2)))
      .then(receive(msg("v1", makoStudio, 1)))
      .expect(seen.get(("v1", makoStudio)) == 1)
}

// ── 順不同・失われる道 / 古い知らせは捨てる ─────────────
module dgramDiscard {
  import field(ORDERED = false, DISCARD_OLDER = true, MAX_BACKLOG = 99, CHECK_LIFE = false).*

  // 知らせが 1 つ失われても、次の知らせで追いつく
  run lostTickIsRecoveredByNextTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(tick(makoStudio))
      .then(lose(msg("v1", makoStudio, 1)))
      .expect(seen.get(("v1", makoStudio)) == 0)
      .then(tick(makoStudio))
      .then(receive(msg("v1", makoStudio, 2)))
      .expect(seen.get(("v1", makoStudio)) == 2)

  // 順番が入れ替わって届いても、手元は古い状態に戻らない
  run outOfOrderIsIgnoredTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .then(receive(msg("v1", makoStudio, 2)))
      .then(receive(msg("v1", makoStudio, 1)))
      .expect(seen.get(("v1", makoStudio)) == 2)

  // 出ていった後に届いた知らせは捨てられる
  run lateMessageAfterLeaveIsDroppedTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(tick(makoStudio))
      .then(leave("v1", makoStudio))
      .then(receive(msg("v1", makoStudio, 1)))
      .expect(and {
        not(isJoined("v1", makoStudio)),
        inflight == Set(),
      })

  // 入り直した後に、前回ぶんの古い知らせが届いても、手元は戻らない
  run staleMessageAfterRejoinTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(tick(makoStudio))
      .then(leave("v1", makoStudio))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .then(join("v1", makoStudio))
      .expect(seen.get(("v1", makoStudio)) == 3)
      .then(receive(msg("v1", makoStudio, 1)))
      .expect(seen.get(("v1", makoStudio)) == 3)

  // 前の代の知らせが、立ち上がり直した後に届くと、手元が field より先に進んでしまう
  run staleLifeGetsAheadTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .then(stop(makoStudio))
      .then(restart(makoStudio))
      .then(join("v1", makoStudio))
      .then(receive(msg("v1", makoStudio, 2)))
      .expect(and {
        seen.get(("v1", makoStudio)) == 2,
        registry.get(makoStudio).tick == 0,
      })
}

// ── 順番どおりの道 / 溜まったぶんも全部届ける ───────────
module streamAll {
  import field(ORDERED = true, DISCARD_OLDER = false, MAX_BACKLOG = 99, CHECK_LIFE = false).*

  // 順番を飛ばしては届かない
  run cannotSkipAheadTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .then(receive(msg("v1", makoStudio, 2)))
      .fail()

  // 知らせは失われない
  run cannotLoseTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(tick(makoStudio))
      .then(lose(msg("v1", makoStudio, 1)))
      .fail()

  // 出ていくと、途中の知らせも消える
  run leaveDropsPendingTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(tick(makoStudio))
      .then(leave("v1", makoStudio))
      .expect(inflight == Set())

  // 止まると、途中の知らせも消える。前の代の知らせが後から届くことはない
  run stopDropsPendingTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .then(stop(makoStudio))
      .expect(inflight == Set())

  // 受け取りが遅い人には全部溜まり、古いものから順に受け取るしかない
  run slowVisitorGetsEverythingTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .expect(pendingFor("v1", makoStudio).size() == 4)
      .then(receive(msg("v1", makoStudio, 1)))
      .expect(and {
        seen.get(("v1", makoStudio)) == 1,
        registry.get(makoStudio).tick == 4,
      })
}

// ── 順番どおりの道 / 溜まったら古いものから送る側が捨てる ─
module streamLatest {
  import field(ORDERED = true, DISCARD_OLDER = false, MAX_BACKLOG = 2, CHECK_LIFE = false).*

  // 受け取りが遅い人には、新しい 2 つだけが残る
  run slowVisitorSkipsToRecentTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .expect(pendingFor("v1", makoStudio) == Set(msg("v1", makoStudio, 3), msg("v1", makoStudio, 4)))
      .then(receive(msg("v1", makoStudio, 3)))
      .then(receive(msg("v1", makoStudio, 4)))
      .expect(seen.get(("v1", makoStudio)) == 4)

  // 捨てられた知らせは届かない
  run droppedMessageIsGoneTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .then(receive(msg("v1", makoStudio, 1)))
      .fail()
}

// ── 順不同・失われる道 / 古い知らせも、前の代の知らせも捨てる ─
module dgramDiscardLife {
  import field(ORDERED = false, DISCARD_OLDER = true, MAX_BACKLOG = 99, CHECK_LIFE = true).*

  // 前の代の知らせが、立ち上がり直した後に届いても、捨てられる
  run staleLifeIsIgnoredTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .then(stop(makoStudio))
      .then(restart(makoStudio))
      .then(join("v1", makoStudio))
      .then(receive(msg("v1", makoStudio, 2)))
      .expect(and {
        seen.get(("v1", makoStudio)) == 0,
        registry.get(makoStudio).tick == 0,
      })
      .then(tick(makoStudio))
      .then(receive(msgOf("v1", makoStudio, 2, 1)))
      .expect(seen.get(("v1", makoStudio)) == 1)

  // 順番が入れ替わって届いても、手元は古い状態に戻らない
  run outOfOrderIsIgnoredTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(tick(makoStudio))
      .then(tick(makoStudio))
      .then(receive(msg("v1", makoStudio, 2)))
      .then(receive(msg("v1", makoStudio, 1)))
      .expect(seen.get(("v1", makoStudio)) == 2)
}
