case class CpmLog(isok: Boolean = false,
                  adsrc: Int,
                  dspMediaId: String,
                  dspAdslotId: String,
                  sid: String,
                  date: String,
                  hour: Int,
                  typed: Int,
                  ideaId: Int,
                  unitId: Int,
                  planId: Int,
                  userId: Int,
                  mediaId: Int,
                  adslotId: Int,
                  adslotType: Int,
                  price: Int,
                  req: Int,
                  fill: Int,
                  imp: Int,
                  click: Int) {

  val key1 = (this.sid, this.typed, this.ideaId)
  val key2 = (this.adsrc, this.dspMediaId, this.dspAdslotId, this.date,
    this.ideaId, this.unitId, this.planId, this.userId,
    this.mediaId, this.adslotId, this.adslotType)

  def sum(another: CpmLog): CpmLog = {
    this.copy(price = this.price + another.price,
      req = this.req + another.req,
      fill = this.fill + another.fill,
      imp = this.imp + another.imp,
      click = this.click + another.click)
  }

  def mkSql: String = {
    "call eval_charge_cpm(" + mediaId + ",0," + adslotId + "," + adslotType + "," + ideaId + "," +
      unitId + "," + planId + "," + userId + ",'" + date + "'," + req + "," + fill + "," + imp + "," +
      click + ",0," + price + ")"
  }

  def mkSqlDsp: String = {
    "call eval_dsp(" + adsrc + ",'" + dspMediaId + "','" + dspAdslotId + "'," + mediaId + ",0," +
      adslotId + "," + adslotType + ",'" + date + "'," + req + "," + fill + "," + imp + "," + click + ")"
  }
}