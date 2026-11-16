中国—东盟跨境智慧物流走廊数字孪生

transport_book 提供手工路段容量和运输承诺。capacity 按路段与时间窗登记容量及版本，
reserve 在一个事务内校验连续路径、货票状态、各路段版本和合计占用；同一货票不能重复预订。
transition 使承诺进入 dispatched、completed 或尚未发运时 cancelled，完成或取消释放占用，
已发运承诺不可撤销。发运前货票资料改变需重新确认。inspect 可查看各时窗剩余容量及历史承诺。

事件关联与处置分派（corridor_twin.incidents）
- 事件（port_congestion、cold_chain、customs_hold、route_closure、weather、capacity）
  记录发生时间、报告时间、来源、节点、路线与观测可信度。credibility 缺省按来源映射。
- analyze 依据三类证据识别影响链：
  1) 路线：路段重合度、上游节点是否出现在后续路线中；路线变化只扩大/收窄影响范围，
     不会把同一口岸同类报告拆成新事件。
  2) 发生顺序：只有先发生的事件才可能成为上游原因；超过因果观察窗口按独立事件处理。
  3) 观测可信度：来源可信度叠加报告迟到衰减与交叉印证，低可信度进入 uncertainties。
- 并案规则：同一节点、同一类型、发生在并案窗口内 -> same_incident；路线/节点可达且强度
  达阈值 -> upstream_cause；证据不足 -> co_incident（同期共发，不并案）；否则 independent。
  冷链异常默认独立建链，只有显式声明 cause_event_id 或同时满足路线、顺序、强度时才并入上游，
  避免把独立冷链异常误并入口岸拥堵。同一口岸不同货类也不会被强行并案。
- 影响范围：从事件与货票路线推断受影响货票（observed 优先，其次 route_inferred），
  冷链事件要求货类吻合；并逐条运输承诺标注 impact（in_transit / route_impacted / in_scope /
  monitoring / closed）与建议动作，形成处置分派、责任跟踪和客户承诺更新依据。
- 更正/迟到/重复：supersedes 指向被更正事件，旧事件退出关联，新事件进入并标注拆分理由；
  重复观测合并为一条（duplicates），不新增工作项；迟到报告进入不确定性与 chain confidence。
- reconcile 把影响链落成工作项（工作项 = 事件集合 + 受影响货票 + 负责人角色）。
  受影响货票或事件集合变化时工作项升版；已确认的工作项重新进入 awaiting_confirmation，
  未确认的保持 open；被更正拆开的工作项留下历史并明确去向。没有事件重叠的新链不会
  继续套用旧节点的工作项。
- confirm 记录负责人、动作、确认人与当时的完整依据快照（basis_hash + rationale；
  依据变化后必须先 reconcile 才能再确认）。resolve 只有在所有事件已解决时才允许关闭。
- withdraw_ticket 只撤销该票运输承诺并重算影响范围，仍影响其他票的事件与工作项继续有效。

运行验收：
python3 -m corridor_twin run --fixture fixtures/acceptance.json --database /tmp/corridor_twin.sqlite3 --output /tmp/corridor_twin-output
python3 -m corridor_twin run --fixture fixtures/incident_chain.json --database /tmp/corridor_twin.sqlite3 --output /tmp/corridor_twin-output

事件关联与分派（可重复导入，幂等）：
python3 -m corridor_twin incidents --database /tmp/corridor_twin.sqlite3 --fixture fixtures/incident_chain.json

核验：
python3 -m corridor_twin verify --database /tmp/corridor_twin.sqlite3 --output /tmp/corridor_twin-output

全量测试：
python3 -m unittest discover -s tests -v

按模块测试：
python3 -m unittest tests.events.test_logistics_event -v
python3 -m unittest tests.incidents.test_incident_chain -v

构建检查：
python3 -m compileall -q corridor_twin tests
