// field のモデル（U1: 自分の field を持つ ／ Origin と Slave の自動同期）
//
// 仕様: docs/spec/01-field.md
// 確かめ方（全部まとめて）:
//   docs/model/check.sh
//
// field は要素（slot）の集まりで、要素ごとに (rev, value) を持つ。
// 正本は Origin（field そのもの）の 1 つだけ。入った人の手元には、その写しの Slave がある。
// Slave は、届いた変動の知らせで、手元の要素を自分で書き換える（自動で同期する）。
//
// このモデルが扱うのは「持つ」「入る（直接 / 中継を通して）」「出ていく」
// 「置き換える（Origin で / Slave から頼んで Origin が確定する）」「届く」「止まる」
// 「立ち上がり直す」と、変動の知らせが届くまでの道のり。
// 要素が何を表すか（照明・カメラ…）は扱わない。名前と、小さな整数の値だけを持つ。
//
// 道のりは切り替えられる（下の const）。組み合わせは末尾の 7 つのモジュール:
//   dgramNaive         順不同・失われる道 / 届いたもので上書き
//   dgramDiscard       順不同・失われる道 / 古い rev は捨てる
//   dgramDiscardLife   dgramDiscard に加えて、前の代の知らせも捨てる
//   streamAll          要素ごとに順番どおりの道 / 溜まったぶんも全部届ける
//   streamLatest       要素ごとに順番どおりの道 / 溜まったら古いものから送る側が捨てる
//   streamLatestEcho   streamLatest に、頼んだ人への応答の道を足す（受け取る側の決まりは無し）
//   streamLatestMerge  streamLatestEcho に、古い rev と前の代を捨てる決まりを足す（実装が選んだもの）

module field {
  // ── 切り替え ────────────────────────────────────────

  // true  = 1 人・1 要素あての変動の知らせは、送った順に、欠けずに届く
  //         （unison の stream channel。要素どうしの順番は決まっていない）
  // false = 順番が入れ替わり、失われうる道（datagram channel に相当）
  const ORDERED: bool

  // true = 受け取る側は、手元より古い（または同じ）rev の知らせを捨てる
  const DISCARD_OLDER: bool

  // 順番どおりの道で、1 人・1 要素あてに溜めておける知らせの上限。
  // 超えたら送る側が古いものから捨てる
  const MAX_BACKLOG: int

  // true = 受け取る側は、field の前の代（立ち上がり直す前）の知らせを捨てる
  const CHECK_LIFE: bool

  // true = Slave から頼んだ置き換えが確定したとき、Origin は頼んだ人に応答も返す。
  // 応答は変動の知らせとは別の道を通るので、知らせとの順番は入れ替わりうる。失われはしない
  const ECHO: bool

  // ── 型 ──────────────────────────────────────────────

  type User = str
  type FieldName = str
  type Visitor = str
  type Slot = str

  // field が住む場所。手元とクラウドは別々の field（裁定 8）
  type Place = Local | Cloud

  // field を指す鍵。「誰の」だけでは足りず「どの field か」まで要る
  type FieldKey = { owner: User, name: FieldName }

  // 要素 1 つの状態。rev は Origin が確定するたびに 1 つ進む番号。
  // rev 0 = まだ一度も置かれていない（値は 0）
  type SlotState = { rev: int, value: int }

  type Field = {
    place: Place,
    // 動いているか
    running: bool,
    // 何代めか。立ち上がり直すたびに 1 つ増える
    life: int,
    // 要素ごとの状態（正本）。状態はメモリ上だけなので、立ち上がり直すと空に戻る
    slots: Slot -> SlotState,
  }

  // 知らせが通る道: 変動の知らせ（Feed）か、頼んだ人への応答（Reply）か
  type Path = Feed | Reply

  // Origin（または中継）から Slave へ向かう途中の知らせ。要素 1 つぶん。
  // seq は送り出した通し番号（同じ道の中で、どちらが先に送られたかを見分ける）
  type Message = {
    to: Visitor, key: FieldKey, life: int, slot: Slot, rev: int, value: int, path: Path, seq: int
  }

  // Slave から Origin へ向かう途中の、置き換えの頼み
  type Request = { from: Visitor, key: FieldKey, life: int, slot: Slot, value: int }

  // ── 状態 ────────────────────────────────────────────

  // 存在している field（Origin）
  var registry: FieldKey -> Field

  // 入っている人の手元の Slave（要素ごとの写し）。
  // 鍵があること = その field に入っていること
  var seen: (Visitor, FieldKey) -> (Slot -> SlotState)

  // 中継: 鍵の人は、Origin からではなく、値の人の Slave を通して入っている
  var via: (Visitor, FieldKey) -> Visitor

  // 送られたが、まだ届いていない知らせ
  var inflight: Set[Message]

  // 頼まれたが、まだ確定していない置き換え
  var requests: Set[Request]

  // 送り出した知らせの数（次の知らせの seq）
  var sent: int

  // 検証用の記録: field が生まれたときの場所
  var bornAt: FieldKey -> Place

  // 検証用の記録: 入ってから今までに手元に持った、要素ごとの rev の最大
  var highest: (Visitor, FieldKey) -> (Slot -> int)

  // ── 検証で使う有限の世界 ────────────────────────────

  pure val USERS: Set[User] = Set("mako", "waki")
  pure val NAMES: Set[FieldName] = Set("studio", "live")
  pure val VISITORS: Set[Visitor] = Set("v1", "v2", "v3")
  pure val PLACES: Set[Place] = Set(Local, Cloud)
  pure val SLOTS: Set[Slot] = Set("light", "camera")
  // 置ける値（0 は「まだ置かれていない」ときの値なので含めない）
  pure val VALUES: Set[int] = Set(1, 2)
  // 1 つの代の中で、1 つの要素を置き換えられる回数
  pure val MAX_REV: int = 3
  pure val MAX_LIFE: int = 3

  // ── 補助 ────────────────────────────────────────────

  pure def larger(a: int, b: int): int = if (a > b) a else b

  // まだ何も置かれていない要素の集まり
  pure val EMPTY: Slot -> SlotState = SLOTS.mapBy(s => { rev: 0, value: 0 })

  pure def revsOf(slots: Slot -> SlotState): Slot -> int = SLOTS.mapBy(s => slots.get(s).rev)

  // 知らせの集まりの中で、一番先に送られたものの seq
  def lowestSeq(ms: Set[Message]): int =
    ms.fold(sent + 1, (acc, m) => if (m.seq < acc) m.seq else acc)

  def hasField(k: FieldKey): bool = registry.keys().contains(k)

  def isRunning(k: FieldKey): bool = hasField(k) and registry.get(k).running

  def isJoined(v: Visitor, k: FieldKey): bool = seen.keys().contains((v, k))

  def joined(k: FieldKey): Set[Visitor] =
    seen.keys().filter(p => p._2 == k).map(p => p._1)

  // Origin から直接、知らせを受け取る人（中継を通していない人）
  def direct(k: FieldKey): Set[Visitor] =
    joined(k).filter(v => not(via.keys().contains((v, k))))

  // v の Slave から、知らせを受け取る人（1 段下）
  def downstream(v: Visitor, k: FieldKey): Set[Visitor] =
    via.keys().filter(p => p._2 == k and via.get(p) == v).map(p => p._1)

  // v と、v を通して（何段でも）入っている人
  def withDownstream(v: Visitor, k: FieldKey): Set[Visitor] =
    VISITORS.fold(Set(v), (acc, i) => acc.union(
      via.keys().filter(p => p._2 == k and acc.contains(via.get(p))).map(p => p._1)))

  def fieldsOf(u: User): Set[FieldKey] =
    registry.keys().filter(k => k.owner == u)

  // ある人あての、ある要素の、まだ届いていない変動の知らせ
  def feedFor(v: Visitor, k: FieldKey, s: Slot): Set[Message] =
    inflight.filter(m => m.path == Feed and m.to == v and m.key == k and m.slot == s)

  // 変動の知らせを audience の全員へ送り出した後の、途中の知らせ全体。
  // 順番どおりの道では、1 人・1 要素あてに溜まるのは MAX_BACKLOG まで。
  // 超えたら送る側が、その要素の一番古いものを捨てる
  def sendFeed(
    msgs: Set[Message], audience: Set[Visitor], k: FieldKey, l: int, s: Slot, r: int, x: int
  ): Set[Message] = {
    val untouched =
      msgs.filter(m => not(m.path == Feed and m.key == k and m.slot == s and audience.contains(m.to)))
    untouched.union(audience.map(v => {
      val mine = msgs
        .filter(m => m.path == Feed and m.to == v and m.key == k and m.slot == s)
        .union(Set({ to: v, key: k, life: l, slot: s, rev: r, value: x, path: Feed, seq: sent }))
      if (ORDERED and mine.size() > MAX_BACKLOG)
        mine.filter(m => m.seq != lowestSeq(mine))
      else
        mine
    }).flatten())
  }

  // ── 操作 ────────────────────────────────────────────

  action init = all {
    registry' = Map(),
    seen' = Map(),
    via' = Map(),
    inflight' = Set(),
    requests' = Set(),
    sent' = 0,
    bornAt' = Map(),
    highest' = Map(),
  }

  // 持つ: ユーザが、場所を選んで、自分の field を作る。要素はまだ空
  action create(u: User, n: FieldName, p: Place): bool = {
    val k = { owner: u, name: n }
    all {
      not(hasField(k)),
      registry' = registry.put(k, { place: p, running: true, life: 1, slots: EMPTY }),
      bornAt' = bornAt.put(k, p),
      seen' = seen,
      via' = via,
      inflight' = inflight,
      requests' = requests,
      sent' = sent,
      highest' = highest,
    }
  }

  // 入る（直接）: Origin の今の状態を手元の Slave に写し、以後の変動を受け始める
  action join(v: Visitor, k: FieldKey): bool = all {
    isRunning(k),
    not(isJoined(v, k)),
    seen' = seen.put((v, k), registry.get(k).slots),
    highest' = highest.put((v, k), revsOf(registry.get(k).slots)),
    registry' = registry,
    via' = via,
    inflight' = inflight,
    requests' = requests,
    sent' = sent,
    bornAt' = bornAt,
  }

  // 入る（中継を通して）: すでに入っている人 r の Slave の今の状態を写し、
  // 以後は r の Slave から変動を受ける
  action joinVia(v: Visitor, r: Visitor, k: FieldKey): bool = all {
    isJoined(r, k),
    not(isJoined(v, k)),
    v != r,
    seen' = seen.put((v, k), seen.get((r, k))),
    highest' = highest.put((v, k), revsOf(seen.get((r, k)))),
    via' = via.put((v, k), r),
    registry' = registry,
    inflight' = inflight,
    requests' = requests,
    sent' = sent,
    bornAt' = bornAt,
  }

  // 出ていく: v と、v を通して（何段でも）入っている人が外れる。field は動き続ける。
  // 順番どおりの道では、道ごと閉じるので、その人たちあての途中の知らせも消える。
  // 応答の道は、どの道のりでも閉じる。すでに送った頼みは Origin に残る
  action leave(v: Visitor, k: FieldKey): bool = {
    val gone = withDownstream(v, k)
    all {
      isJoined(v, k),
      seen' = seen.keys().filter(p => not(p._2 == k and gone.contains(p._1))).mapBy(p => seen.get(p)),
      highest' =
        highest.keys().filter(p => not(p._2 == k and gone.contains(p._1))).mapBy(p => highest.get(p)),
      via' = via.keys().filter(p => not(p._2 == k and gone.contains(p._1))).mapBy(p => via.get(p)),
      inflight' =
        inflight.filter(m => not(m.key == k and gone.contains(m.to) and (ORDERED or m.path == Reply))),
      requests' = requests,
      sent' = sent,
      registry' = registry,
      bornAt' = bornAt,
    }
  }

  // Origin が要素 s を x に置き換えて確定する（その要素の rev が 1 つ進む）。
  // 直接入っている全員へ変動の知らせを、echoTo の人へ応答を送り出す。届くのは後（receive）
  action commitAt(k: FieldKey, s: Slot, x: int, echoTo: Set[Visitor]): bool = {
    val f = registry.get(k)
    val r = f.slots.get(s).rev + 1
    all {
      isRunning(k),
      r <= MAX_REV,
      registry' = registry.set(k, { ...f, slots: f.slots.set(s, { rev: r, value: x }) }),
      inflight' = sendFeed(inflight, direct(k), k, f.life, s, r, x).union(
        echoTo.map(w => { to: w, key: k, life: f.life, slot: s, rev: r, value: x, path: Reply, seq: sent })),
      sent' = sent + 1,
      seen' = seen,
      via' = via,
      highest' = highest,
      bornAt' = bornAt,
    }
  }

  // 置き換える（Origin で）: Origin の要素を直接置き換える
  action setSlot(k: FieldKey, s: Slot, x: int): bool = all {
    commitAt(k, s, x, Set()),
    requests' = requests,
  }

  // 置き換えを頼む（Slave から）: 手元はまだ変えない。頼みは Origin へ向かう
  action requestSet(v: Visitor, k: FieldKey, s: Slot, x: int): bool = all {
    isJoined(v, k),
    requests' = requests.union(Set({ from: v, key: k, life: registry.get(k).life, slot: s, value: x })),
    registry' = registry,
    seen' = seen,
    via' = via,
    inflight' = inflight,
    sent' = sent,
    bornAt' = bornAt,
    highest' = highest,
  }

  // 確定する: Origin が、届いた頼みを 1 つ確定する。
  // 頼みどうしの順番は決まっていない（後から確定したものが勝つ）
  action commit(q: Request): bool = all {
    requests.contains(q),
    q.life == registry.get(q.key).life,
    commitAt(q.key, q.slot, q.value, if (ECHO and isJoined(q.from, q.key)) Set(q.from) else Set()),
    requests' = requests.exclude(Set(q)),
  }

  // 届く: 途中の知らせが 1 つ届く。手元の Slave が、その要素を書き換える。
  // 中継: 受け入れた変動は、自分を通して入っている人へ、そのまま送り出す（rev は Origin が振ったまま）
  action receive(m: Message): bool = {
    val p = (m.to, m.key)
    val here = isJoined(m.to, m.key)
    // 入っている人が知っている代は、field の今の代と同じ
    // （field が止まると全員が外れ、入り直したときに今の代を受け取るため）
    val fromOldLife = CHECK_LIFE and here and m.life != registry.get(m.key).life
    val older = DISCARD_OLDER and here and m.rev <= seen.get(p).get(m.slot).rev
    // すでに出ていった人あての知らせは捨てる
    val accept = here and not(fromOldLife) and not(older)
    val rest = inflight.exclude(Set(m))
    all {
      inflight.contains(m),
      // 順番どおりの道では、1 人・1 要素あての変動の知らせは、送り出した順にしか届かない
      (ORDERED and m.path == Feed) implies (m.seq == lowestSeq(feedFor(m.to, m.key, m.slot))),
      seen' =
        if (accept) seen.set(p, seen.get(p).set(m.slot, { rev: m.rev, value: m.value }))
        else seen,
      highest' =
        if (accept) highest.set(p, highest.get(p).set(m.slot, larger(highest.get(p).get(m.slot), m.rev)))
        else highest,
      inflight' =
        if (accept) sendFeed(rest, downstream(m.to, m.key), m.key, m.life, m.slot, m.rev, m.value)
        else rest,
      sent' = if (accept) sent + 1 else sent,
      registry' = registry,
      via' = via,
      requests' = requests,
      bornAt' = bornAt,
    }
  }

  // 失われる: 途中の変動の知らせが 1 つ、届かずに消える。
  // 順番どおりの道では起きない。応答の道でも起きない
  action lose(m: Message): bool = all {
    not(ORDERED),
    m.path == Feed,
    inflight.contains(m),
    inflight' = inflight.exclude(Set(m)),
    seen' = seen,
    via' = via,
    highest' = highest,
    requests' = requests,
    sent' = sent,
    registry' = registry,
    bornAt' = bornAt,
  }

  // 止まる: field のプロセスが止まる。入っていた人は全員外れ、確定していない頼みも消える。
  // 順番どおりの道では、道ごと閉じるので、途中の知らせも消える。
  // 順不同の道では、送り出し済みの変動の知らせは残る
  action stop(k: FieldKey): bool = all {
    isRunning(k),
    registry' = registry.set(k, { ...registry.get(k), running: false }),
    seen' = seen.keys().filter(p => p._2 != k).mapBy(p => seen.get(p)),
    highest' = highest.keys().filter(p => p._2 != k).mapBy(p => highest.get(p)),
    via' = via.keys().filter(p => p._2 != k).mapBy(p => via.get(p)),
    inflight' = inflight.filter(m => not(m.key == k and (ORDERED or m.path == Reply))),
    requests' = requests.filter(q => q.key != k),
    sent' = sent,
    bornAt' = bornAt,
  }

  // 立ち上がり直す: 同じ field が、同じ場所で、次の代として動き出す。
  // 要素は空から（状態はメモリ上だけ）
  action restart(k: FieldKey): bool = all {
    hasField(k),
    not(registry.get(k).running),
    registry.get(k).life < MAX_LIFE,
    registry' = registry.set(k, {
      ...registry.get(k), running: true, life: registry.get(k).life + 1, slots: EMPTY
    }),
    seen' = seen,
    via' = via,
    highest' = highest,
    inflight' = inflight,
    requests' = requests,
    sent' = sent,
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
    nondet r = oneOf(VISITORS)
    nondet u = oneOf(USERS)
    nondet n = oneOf(NAMES)
    joinVia(v, r, { owner: u, name: n }),

    nondet v = oneOf(VISITORS)
    nondet u = oneOf(USERS)
    nondet n = oneOf(NAMES)
    leave(v, { owner: u, name: n }),

    nondet u = oneOf(USERS)
    nondet n = oneOf(NAMES)
    nondet s = oneOf(SLOTS)
    nondet x = oneOf(VALUES)
    setSlot({ owner: u, name: n }, s, x),

    nondet v = oneOf(VISITORS)
    nondet u = oneOf(USERS)
    nondet n = oneOf(NAMES)
    nondet s = oneOf(SLOTS)
    nondet x = oneOf(VALUES)
    requestSet(v, { owner: u, name: n }, s, x),

    all {
      requests != Set(),
      nondet q = oneOf(requests)
      commit(q),
    },

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

  // 手元の rev が、Origin の rev より先に進むことはない（要素ごと）
  val seenNeverAhead =
    seen.keys().forall(p => SLOTS.forall(s =>
      seen.get(p).get(s).rev <= registry.get(p._2).slots.get(s).rev))

  // 途中の知らせが、未来のことを言うことはない
  // （今の代の知らせは今の rev 以下。先の代の知らせは無い）
  val inflightNeverAhead =
    inflight.forall(m => and {
      hasField(m.key),
      m.life <= registry.get(m.key).life,
      (m.life == registry.get(m.key).life) implies
        (m.rev <= registry.get(m.key).slots.get(m.slot).rev),
    })

  // 手元の rev が Origin と同じなら、値も同じ（rev が同じなのに値が食い違うことはない）
  val sameRevSameValue =
    seen.keys().forall(p => SLOTS.forall(s => {
      val mine = seen.get(p).get(s)
      val origin = registry.get(p._2).slots.get(s)
      (mine.rev == origin.rev) implies (mine.value == origin.value)
    }))

  // 止まっている field に入っている人はいない
  val stoppedFieldHasNoOne =
    seen.keys().forall(p => isRunning(p._2))

  // field は生まれた場所に住み続ける（移さない）
  val placeNeverChanges =
    registry.keys().forall(k => registry.get(k).place == bornAt.get(k))

  // 存在しない field に入っている人はいない
  val noOneInMissingField =
    seen.keys().forall(p => hasField(p._2))

  // 中継を通して入っている人の中継先は、同じ field に入っている別の人
  val relayIsJoined =
    via.keys().forall(p => and {
      isJoined(p._1, p._2),
      isJoined(via.get(p), p._2),
      via.get(p) != p._1,
    })

  val allInvariants = all {
    seenNeverAhead,
    inflightNeverAhead,
    sameRevSameValue,
    placeNeverChanges,
    noOneInMissingField,
    stoppedFieldHasNoOne,
    relayIsJoined,
  }

  // ── 道によって成り立ったり破れたりすること ──────────
  // どの組み合わせで成り立つかは docs/model/check.sh の表が正

  // 手元の rev は戻らない（古い知らせで上書きされない。要素ごと）
  val seenNeverGoesBack =
    seen.keys().forall(p => SLOTS.forall(s => seen.get(p).get(s).rev == highest.get(p).get(s)))

  // その field あての途中の知らせがもう無いなら、入っている全員の手元は Origin と同じ（全要素）
  val quietMeansCurrent =
    registry.keys().forall(k =>
      (inflight.filter(m => m.key == k) == Set()) implies
        joined(k).forall(v => seen.get((v, k)) == registry.get(k).slots))

  // 1 人・1 要素あてに溜まっている変動の知らせは 2 つまで
  val backlogAtMostTwo =
    seen.keys().forall(p => SLOTS.forall(s => feedFor(p._1, p._2, s).size() <= 2))

  // 入っている人は、常に Origin の今の状態を持っている（どの道でも破れる）
  val alwaysCurrent =
    seen.keys().forall(p => seen.get(p) == registry.get(p._2).slots)

  // ── 場面の確かめで使う補助 ──────────────────────────

  pure val makoStudio: FieldKey = { owner: "mako", name: "studio" }
  pure val makoLive: FieldKey = { owner: "mako", name: "live" }
  pure val wakiStudio: FieldKey = { owner: "waki", name: "studio" }

  // 手元の Slave の、ある要素の rev / 値
  def revOf(v: Visitor, k: FieldKey, s: Slot): int = seen.get((v, k)).get(s).rev
  def valueOf(v: Visitor, k: FieldKey, s: Slot): int = seen.get((v, k)).get(s).value

  // Origin の、ある要素の rev / 値
  def originRev(k: FieldKey, s: Slot): int = registry.get(k).slots.get(s).rev
  def originValue(k: FieldKey, s: Slot): int = registry.get(k).slots.get(s).value

  // ある人あての、ある要素の、まだ届いていない変動の知らせの rev
  def feedRevs(v: Visitor, k: FieldKey, s: Slot): Set[int] = feedFor(v, k, s).map(m => m.rev)

  // 条件に合う途中の知らせを 1 つ届ける
  action deliver(v: Visitor, k: FieldKey, s: Slot, l: int, r: int, path: Path): bool = {
    val ms = inflight.filter(m => and {
      m.to == v, m.key == k, m.slot == s, m.life == l, m.rev == r, m.path == path
    })
    all {
      ms != Set(),
      nondet m = oneOf(ms)
      receive(m),
    }
  }

  // 1 代めの変動の知らせが届く
  action deliverFeed(v: Visitor, k: FieldKey, s: Slot, r: int): bool = deliver(v, k, s, 1, r, Feed)

  // 何代めかを指定した変動の知らせが届く
  action deliverFeedOf(v: Visitor, k: FieldKey, l: int, s: Slot, r: int): bool =
    deliver(v, k, s, l, r, Feed)

  // 1 代めの応答が届く
  action deliverReply(v: Visitor, k: FieldKey, s: Slot, r: int): bool = deliver(v, k, s, 1, r, Reply)

  // 1 代めの変動の知らせが失われる
  action loseFeed(v: Visitor, k: FieldKey, s: Slot, r: int): bool = {
    val ms = inflight.filter(m => and {
      m.to == v, m.key == k, m.slot == s, m.life == 1, m.rev == r, m.path == Feed
    })
    all {
      ms != Set(),
      nondet m = oneOf(ms)
      lose(m),
    }
  }

  // v が頼んだ置き換えを、Origin が確定する
  action commitOf(v: Visitor, k: FieldKey, s: Slot, x: int): bool =
    commit({ from: v, key: k, life: registry.get(k).life, slot: s, value: x })

  // ── 場面ごとの確かめ（どの道でも通る）──────────────

  // 誰も入っていなくても、Origin の要素は置き換えられる。要素ごとに rev が進む
  run setAdvancesWithNobodyTest =
    init
      .then(create("mako", "studio", Local))
      .then(setSlot(makoStudio, "light", 1))
      .then(setSlot(makoStudio, "light", 2))
      .then(setSlot(makoStudio, "camera", 1))
      .expect(and {
        originRev(makoStudio, "light") == 2,
        originValue(makoStudio, "light") == 2,
        originRev(makoStudio, "camera") == 1,
        joined(makoStudio) == Set(),
        inflight == Set(),
      })

  // 入った人は、その時点の状態をすぐ受け取る。以後の変動は届いたときに反映される
  run joinGetsSnapshotThenUpdatesTest =
    init
      .then(create("mako", "studio", Local))
      .then(setSlot(makoStudio, "light", 1))
      .then(join("v1", makoStudio))
      .expect(and {
        revOf("v1", makoStudio, "light") == 1,
        valueOf("v1", makoStudio, "light") == 1,
        revOf("v1", makoStudio, "camera") == 0,
      })
      .then(setSlot(makoStudio, "light", 2))
      .expect(valueOf("v1", makoStudio, "light") == 1)
      .then(deliverFeed("v1", makoStudio, "light", 2))
      .expect(and {
        revOf("v1", makoStudio, "light") == 2,
        valueOf("v1", makoStudio, "light") == 2,
      })

  // 要素ごとに rev は別々に進み、知らせも要素ごとに送られる
  run slotsAreIndependentTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(setSlot(makoStudio, "camera", 2))
      .then(setSlot(makoStudio, "light", 2))
      .expect(and {
        originRev(makoStudio, "light") == 2,
        originRev(makoStudio, "camera") == 1,
        feedRevs("v1", makoStudio, "light") == Set(1, 2),
        feedRevs("v1", makoStudio, "camera") == Set(1),
      })

  // 要素どうしは、届く順番が入れ替わっても、手元の Slave で合流する
  run slotsMergeInAnyOrderTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(setSlot(makoStudio, "camera", 2))
      .then(deliverFeed("v1", makoStudio, "camera", 1))
      .expect(and {
        revOf("v1", makoStudio, "camera") == 1,
        revOf("v1", makoStudio, "light") == 0,
      })
      .then(deliverFeed("v1", makoStudio, "light", 1))
      .expect(seen.get(("v1", makoStudio)) == registry.get(makoStudio).slots)

  // Slave から頼んだ置き換えは、Origin が確定してから、入っている全員に届く。
  // 頼んだだけでは、Origin も手元も変わらない
  run slaveSetIsConfirmedByOriginTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(join("v2", makoStudio))
      .then(requestSet("v1", makoStudio, "light", 2))
      .expect(and {
        originRev(makoStudio, "light") == 0,
        revOf("v1", makoStudio, "light") == 0,
        inflight == Set(),
      })
      .then(commitOf("v1", makoStudio, "light", 2))
      .expect(and {
        originRev(makoStudio, "light") == 1,
        originValue(makoStudio, "light") == 2,
        feedRevs("v1", makoStudio, "light") == Set(1),
        feedRevs("v2", makoStudio, "light") == Set(1),
      })
      .then(deliverFeed("v1", makoStudio, "light", 1))
      .then(deliverFeed("v2", makoStudio, "light", 1))
      .expect(and {
        valueOf("v1", makoStudio, "light") == 2,
        valueOf("v2", makoStudio, "light") == 2,
      })

  // 同じ要素を 2 人が頼むと、Origin が後から確定したほうが勝ち、全員がそれに揃う
  run originDecidesConcurrentSetsTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(join("v2", makoStudio))
      .then(requestSet("v1", makoStudio, "light", 1))
      .then(requestSet("v2", makoStudio, "light", 2))
      .then(commitOf("v2", makoStudio, "light", 2))
      .then(commitOf("v1", makoStudio, "light", 1))
      .expect(and {
        originRev(makoStudio, "light") == 2,
        originValue(makoStudio, "light") == 1,
      })
      .then(deliverFeed("v1", makoStudio, "light", 1))
      .then(deliverFeed("v1", makoStudio, "light", 2))
      .then(deliverFeed("v2", makoStudio, "light", 1))
      .then(deliverFeed("v2", makoStudio, "light", 2))
      .expect(and {
        valueOf("v1", makoStudio, "light") == 1,
        valueOf("v2", makoStudio, "light") == 1,
      })

  // 入っていない人は、置き換えを頼めない
  run requestWithoutJoinFailsTest =
    init
      .then(create("mako", "studio", Local))
      .then(requestSet("v1", makoStudio, "light", 1))
      .fail()

  // 止まると、確定していない頼みは消える
  run stopDropsRequestsTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(requestSet("v1", makoStudio, "light", 1))
      .then(stop(makoStudio))
      .expect(requests == Set())

  // 中継: 入っている人の Slave を通して入ると、変動はその Slave を経て届く。
  // rev は Origin が振ったまま
  run relayDeliversDownstreamTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(joinVia("v2", "v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .expect(and {
        feedRevs("v1", makoStudio, "light") == Set(1),
        feedRevs("v2", makoStudio, "light") == Set(),
      })
      .then(deliverFeed("v1", makoStudio, "light", 1))
      .expect(feedRevs("v2", makoStudio, "light") == Set(1))
      .then(deliverFeed("v2", makoStudio, "light", 1))
      .expect(and {
        revOf("v2", makoStudio, "light") == 1,
        valueOf("v2", makoStudio, "light") == 1,
      })

  // 中継は何段でも重ねられる
  run relayChainTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(joinVia("v2", "v1", makoStudio))
      .then(joinVia("v3", "v2", makoStudio))
      .then(setSlot(makoStudio, "camera", 2))
      .then(deliverFeed("v1", makoStudio, "camera", 1))
      .then(deliverFeed("v2", makoStudio, "camera", 1))
      .then(deliverFeed("v3", makoStudio, "camera", 1))
      .expect(and {
        revOf("v3", makoStudio, "camera") == 1,
        valueOf("v3", makoStudio, "camera") == 2,
        inflight == Set(),
      })

  // 中継を通して入ると、中継の手元の状態を受け取る（Origin より遅れていることもある）。
  // 中継が追いつけば、それを通して入った人にも届く
  run relayJoinGetsRelayStateTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(joinVia("v2", "v1", makoStudio))
      .expect(revOf("v2", makoStudio, "light") == 0)
      .then(deliverFeed("v1", makoStudio, "light", 1))
      .then(deliverFeed("v2", makoStudio, "light", 1))
      .expect(revOf("v2", makoStudio, "light") == 1)

  // 中継を通して入っている人も、置き換えを頼める。頼みは Origin で確定する
  run relayedSlaveCanRequestTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(joinVia("v2", "v1", makoStudio))
      .then(requestSet("v2", makoStudio, "light", 2))
      .then(commitOf("v2", makoStudio, "light", 2))
      .expect(and {
        originValue(makoStudio, "light") == 2,
        feedRevs("v1", makoStudio, "light") == Set(1),
      })
      .then(deliverFeed("v1", makoStudio, "light", 1))
      .then(deliverFeed("v2", makoStudio, "light", 1))
      .expect(valueOf("v2", makoStudio, "light") == 2)

  // 中継が出ていくと、それを通して入っていた人も（何段でも）外れる
  run relayLeaveDropsDownstreamTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(joinVia("v2", "v1", makoStudio))
      .then(joinVia("v3", "v2", makoStudio))
      .then(leave("v2", makoStudio))
      .expect(and {
        joined(makoStudio) == Set("v1"),
        via == Map(),
      })
      .then(leave("v1", makoStudio))
      .expect(joined(makoStudio) == Set())

  // 入っていない人を通しては入れない
  run joinViaOutsiderFailsTest =
    init
      .then(create("mako", "studio", Local))
      .then(joinVia("v2", "v1", makoStudio))
      .fail()

  // 出ていった人には知らせが送られず、field は動き続ける
  run leaveStopsDeliveryTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(leave("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .expect(and {
        originRev(makoStudio, "light") == 1,
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
      .then(setSlot(makoStudio, "light", 1))
      .then(setSlot(makoStudio, "light", 2))
      .expect(and {
        originRev(makoStudio, "light") == 2,
        originRev(makoLive, "light") == 0,
        revOf("v1", makoLive, "light") == 0,
        inflight == Set(),
      })

  // 同じ名前でも、持ち主が違えば別の field
  run sameNameDifferentOwnerTest =
    init
      .then(create("mako", "studio", Local))
      .then(create("waki", "studio", Cloud))
      .then(setSlot(makoStudio, "light", 1))
      .expect(and {
        originRev(makoStudio, "light") == 1,
        originRev(wakiStudio, "light") == 0,
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
      .then(joinVia("v3", "v2", makoStudio))
      .then(stop(makoStudio))
      .expect(and {
        joined(makoStudio) == Set(),
        via == Map(),
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

  // 止まっている field の要素は置き換えられない
  run cannotSetStoppedTest =
    init
      .then(create("mako", "studio", Local))
      .then(stop(makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .fail()

  // 立ち上がり直しても、同じ field（同じ持ち主・名前・場所）。代が進み、要素は空から
  run restartKeepsIdentityTest =
    init
      .then(create("mako", "studio", Cloud))
      .then(setSlot(makoStudio, "light", 1))
      .then(setSlot(makoStudio, "camera", 2))
      .then(stop(makoStudio))
      .then(restart(makoStudio))
      .expect(and {
        fieldsOf("mako") == Set(makoStudio),
        registry.get(makoStudio).place == Cloud,
        registry.get(makoStudio).life == 2,
        registry.get(makoStudio).slots == EMPTY,
        isRunning(makoStudio),
      })

  // 立ち上がり直した後に入り直すと、新しい代の状態を受け取る
  run rejoinAfterRestartTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(deliverFeed("v1", makoStudio, "light", 1))
      .then(stop(makoStudio))
      .then(restart(makoStudio))
      .then(join("v1", makoStudio))
      .expect(revOf("v1", makoStudio, "light") == 0)
      .then(setSlot(makoStudio, "light", 2))
      .then(deliverFeedOf("v1", makoStudio, 2, "light", 1))
      .expect(and {
        revOf("v1", makoStudio, "light") == 1,
        valueOf("v1", makoStudio, "light") == 2,
      })

  // 動いている field は立ち上がり直せない
  run cannotRestartRunningTest =
    init
      .then(create("mako", "studio", Local))
      .then(restart(makoStudio))
      .fail()
}

// ── 順不同・失われる道 / 届いたもので上書き ─────────────
module dgramNaive {
  import field(
    ORDERED = false, DISCARD_OLDER = false, MAX_BACKLOG = 99, CHECK_LIFE = false, ECHO = false
  ).*

  // 知らせが 1 つ失われても、同じ要素の次の知らせで追いつく
  run lostUpdateIsRecoveredByNextTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(loseFeed("v1", makoStudio, "light", 1))
      .expect(revOf("v1", makoStudio, "light") == 0)
      .then(setSlot(makoStudio, "light", 2))
      .then(deliverFeed("v1", makoStudio, "light", 2))
      .expect(revOf("v1", makoStudio, "light") == 2)

  // 上書きの決まりだと、順番が入れ替わったときに手元が古い状態に戻る
  run outOfOrderGoesBackTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(setSlot(makoStudio, "light", 2))
      .then(deliverFeed("v1", makoStudio, "light", 2))
      .then(deliverFeed("v1", makoStudio, "light", 1))
      .expect(revOf("v1", makoStudio, "light") == 1)
}

// ── 順不同・失われる道 / 古い rev は捨てる ─────────────
module dgramDiscard {
  import field(
    ORDERED = false, DISCARD_OLDER = true, MAX_BACKLOG = 99, CHECK_LIFE = false, ECHO = false
  ).*

  // 知らせが 1 つ失われても、同じ要素の次の知らせで追いつく
  run lostUpdateIsRecoveredByNextTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(loseFeed("v1", makoStudio, "light", 1))
      .expect(revOf("v1", makoStudio, "light") == 0)
      .then(setSlot(makoStudio, "light", 2))
      .then(deliverFeed("v1", makoStudio, "light", 2))
      .expect(revOf("v1", makoStudio, "light") == 2)

  // 順番が入れ替わって届いても、手元は古い状態に戻らない
  run outOfOrderIsIgnoredTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(setSlot(makoStudio, "light", 2))
      .then(deliverFeed("v1", makoStudio, "light", 2))
      .then(deliverFeed("v1", makoStudio, "light", 1))
      .expect(revOf("v1", makoStudio, "light") == 2)

  // 出ていった後に届いた知らせは捨てられる
  run lateMessageAfterLeaveIsDroppedTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(leave("v1", makoStudio))
      .then(deliverFeed("v1", makoStudio, "light", 1))
      .expect(and {
        not(isJoined("v1", makoStudio)),
        inflight == Set(),
      })

  // 入り直した後に、前回ぶんの古い知らせが届いても、手元は戻らない
  run staleMessageAfterRejoinTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(leave("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 2))
      .then(setSlot(makoStudio, "light", 1))
      .then(join("v1", makoStudio))
      .expect(revOf("v1", makoStudio, "light") == 3)
      .then(deliverFeed("v1", makoStudio, "light", 1))
      .expect(revOf("v1", makoStudio, "light") == 3)

  // 前の代の知らせが、立ち上がり直した後に届くと、手元が Origin より先に進んでしまう
  run staleLifeGetsAheadTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(setSlot(makoStudio, "light", 2))
      .then(stop(makoStudio))
      .then(restart(makoStudio))
      .then(join("v1", makoStudio))
      .then(deliverFeed("v1", makoStudio, "light", 2))
      .expect(and {
        revOf("v1", makoStudio, "light") == 2,
        originRev(makoStudio, "light") == 0,
      })
}

// ── 順不同・失われる道 / 古い rev も、前の代の知らせも捨てる ─
module dgramDiscardLife {
  import field(
    ORDERED = false, DISCARD_OLDER = true, MAX_BACKLOG = 99, CHECK_LIFE = true, ECHO = false
  ).*

  // 前の代の知らせが、立ち上がり直した後に届いても、捨てられる
  run staleLifeIsIgnoredTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(setSlot(makoStudio, "light", 2))
      .then(stop(makoStudio))
      .then(restart(makoStudio))
      .then(join("v1", makoStudio))
      .then(deliverFeed("v1", makoStudio, "light", 2))
      .expect(and {
        revOf("v1", makoStudio, "light") == 0,
        originRev(makoStudio, "light") == 0,
      })
      .then(setSlot(makoStudio, "light", 1))
      .then(deliverFeedOf("v1", makoStudio, 2, "light", 1))
      .expect(revOf("v1", makoStudio, "light") == 1)

  // 順番が入れ替わって届いても、手元は古い状態に戻らない
  run outOfOrderIsIgnoredTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(setSlot(makoStudio, "light", 2))
      .then(deliverFeed("v1", makoStudio, "light", 2))
      .then(deliverFeed("v1", makoStudio, "light", 1))
      .expect(revOf("v1", makoStudio, "light") == 2)

  // ある要素の最後の知らせが失われると、別の要素の知らせが届いても、その要素は古いまま
  run lostSlotStaysBehindTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(loseFeed("v1", makoStudio, "light", 1))
      .then(setSlot(makoStudio, "camera", 1))
      .then(deliverFeed("v1", makoStudio, "camera", 1))
      .expect(and {
        inflight == Set(),
        revOf("v1", makoStudio, "camera") == 1,
        revOf("v1", makoStudio, "light") == 0,
        originRev(makoStudio, "light") == 1,
      })
}

// ── 要素ごとに順番どおりの道 / 溜まったぶんも全部届ける ─
module streamAll {
  import field(
    ORDERED = true, DISCARD_OLDER = false, MAX_BACKLOG = 99, CHECK_LIFE = false, ECHO = false
  ).*

  // 同じ要素の知らせは、順番を飛ばしては届かない
  run cannotSkipAheadTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(setSlot(makoStudio, "light", 2))
      .then(deliverFeed("v1", makoStudio, "light", 2))
      .fail()

  // 知らせは失われない
  run cannotLoseTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(loseFeed("v1", makoStudio, "light", 1))
      .fail()

  // 出ていくと、途中の知らせも消える
  run leaveDropsPendingTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(leave("v1", makoStudio))
      .expect(inflight == Set())

  // 止まると、途中の知らせも消える。前の代の知らせが後から届くことはない
  run stopDropsPendingTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(setSlot(makoStudio, "camera", 1))
      .then(stop(makoStudio))
      .expect(inflight == Set())

  // 受け取りが遅い人には全部溜まり、古いものから順に受け取るしかない
  run slowVisitorGetsEverythingTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(setSlot(makoStudio, "light", 2))
      .then(setSlot(makoStudio, "light", 1))
      .expect(feedRevs("v1", makoStudio, "light") == Set(1, 2, 3))
      .then(deliverFeed("v1", makoStudio, "light", 1))
      .expect(and {
        revOf("v1", makoStudio, "light") == 1,
        originRev(makoStudio, "light") == 3,
      })
}

// ── 要素ごとに順番どおりの道 / 溜まったら古いものから送る側が捨てる ─
module streamLatest {
  import field(
    ORDERED = true, DISCARD_OLDER = false, MAX_BACKLOG = 2, CHECK_LIFE = false, ECHO = false
  ).*

  // 受け取りが遅い人には、同じ要素の新しい 2 つだけが残る
  run slowVisitorSkipsToRecentTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(setSlot(makoStudio, "light", 2))
      .then(setSlot(makoStudio, "light", 1))
      .expect(feedRevs("v1", makoStudio, "light") == Set(2, 3))
      .then(deliverFeed("v1", makoStudio, "light", 2))
      .then(deliverFeed("v1", makoStudio, "light", 3))
      .expect(revOf("v1", makoStudio, "light") == 3)

  // 間引きは要素ごと。ほかの要素の知らせは捨てられない
  run backlogIsPerSlotTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "camera", 1))
      .then(setSlot(makoStudio, "light", 1))
      .then(setSlot(makoStudio, "light", 2))
      .then(setSlot(makoStudio, "light", 1))
      .expect(and {
        feedRevs("v1", makoStudio, "light") == Set(2, 3),
        feedRevs("v1", makoStudio, "camera") == Set(1),
      })

  // 捨てられた知らせは届かない
  run droppedMessageIsGoneTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(setSlot(makoStudio, "light", 1))
      .then(setSlot(makoStudio, "light", 2))
      .then(setSlot(makoStudio, "light", 1))
      .then(deliverFeed("v1", makoStudio, "light", 1))
      .fail()
}

// ── streamLatest に、頼んだ人への応答の道を足す（受け取る側の決まりは無し）─
module streamLatestEcho {
  import field(
    ORDERED = true, DISCARD_OLDER = false, MAX_BACKLOG = 2, CHECK_LIFE = false, ECHO = true
  ).*

  // 頼んだ人には、変動の知らせとは別に、応答も届く
  run requesterGetsReplyTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(join("v2", makoStudio))
      .then(requestSet("v1", makoStudio, "light", 2))
      .then(commitOf("v1", makoStudio, "light", 2))
      .then(deliverReply("v1", makoStudio, "light", 1))
      .expect(and {
        valueOf("v1", makoStudio, "light") == 2,
        valueOf("v2", makoStudio, "light") == 0,
      })

  // 応答が、後の変動の知らせに追い越されると、頼んだ人の手元が古い状態に戻る
  run lateReplyGoesBackTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(requestSet("v1", makoStudio, "light", 1))
      .then(commitOf("v1", makoStudio, "light", 1))
      .then(setSlot(makoStudio, "light", 2))
      .then(deliverFeed("v1", makoStudio, "light", 1))
      .then(deliverFeed("v1", makoStudio, "light", 2))
      .then(deliverReply("v1", makoStudio, "light", 1))
      .expect(and {
        inflight == Set(),
        revOf("v1", makoStudio, "light") == 1,
        originRev(makoStudio, "light") == 2,
      })
}

// ── streamLatestEcho に、古い rev と前の代を捨てる決まりを足す（実装が選んだもの）─
module streamLatestMerge {
  import field(
    ORDERED = true, DISCARD_OLDER = true, MAX_BACKLOG = 2, CHECK_LIFE = true, ECHO = true
  ).*

  // 応答が後から届いても、捨てられる。手元は戻らない
  run lateReplyIsIgnoredTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(requestSet("v1", makoStudio, "light", 1))
      .then(commitOf("v1", makoStudio, "light", 1))
      .then(setSlot(makoStudio, "light", 2))
      .then(deliverFeed("v1", makoStudio, "light", 1))
      .then(deliverFeed("v1", makoStudio, "light", 2))
      .then(deliverReply("v1", makoStudio, "light", 1))
      .expect(and {
        inflight == Set(),
        revOf("v1", makoStudio, "light") == 2,
        valueOf("v1", makoStudio, "light") == 2,
      })

  // 応答が先に届けば、頼んだ人は変動の知らせを待たずに確定した値を持つ。
  // 後から届いた同じ rev の知らせは、何も変えない
  run replyFirstThenFeedTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(requestSet("v1", makoStudio, "light", 2))
      .then(commitOf("v1", makoStudio, "light", 2))
      .then(deliverReply("v1", makoStudio, "light", 1))
      .expect(and {
        revOf("v1", makoStudio, "light") == 1,
        valueOf("v1", makoStudio, "light") == 2,
      })
      .then(deliverFeed("v1", makoStudio, "light", 1))
      .expect(and {
        inflight == Set(),
        revOf("v1", makoStudio, "light") == 1,
      })

  // 中継が頼んで応答を先に受け取ると、それを通して入っている人へも送り出す。
  // 後から届いた変動の知らせは捨てられ、二重には送られない
  run relayEchoIsForwardedOnceTest =
    init
      .then(create("mako", "studio", Local))
      .then(join("v1", makoStudio))
      .then(joinVia("v2", "v1", makoStudio))
      .then(requestSet("v1", makoStudio, "light", 2))
      .then(commitOf("v1", makoStudio, "light", 2))
      .then(deliverReply("v1", makoStudio, "light", 1))
      .expect(feedRevs("v2", makoStudio, "light") == Set(1))
      .then(deliverFeed("v1", makoStudio, "light", 1))
      .expect(feedRevs("v2", makoStudio, "light") == Set(1))
      .then(deliverFeed("v2", makoStudio, "light", 1))
      .expect(and {
        inflight == Set(),
        valueOf("v2", makoStudio, "light") == 2,
      })
}
