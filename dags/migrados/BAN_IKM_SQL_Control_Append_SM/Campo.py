# DEFINICION DE LA CLASE CAMPO
# Esto es una parte de las definiciones que se hacen en la funcion 100
# pero se extrajo parte de la definicion que es independiente del contexto donde
# se ejecuta la funcion

# USO:
# Antes de hacer uso de la función Campo.getCampos hay que asignarle un valor
# a Campo.vListaCampos para que funcione correctamente. Esto puede extraerse
# del contexto en el que se está utilizando la función
class Campo:
	vListaCampos = []

	def __init__(self) -> None:
		self.vNombreCampo = ""
		self.vTipoCampo = ""
		self.vNumero = 0
		self.vDescCampo = ""
		self.vNombreCampoUltHab = ""
		self.vNombreCampoAcum = ""
		self.vNombreCampoCurr = ""
		self.vNombreCampoPrior = ""
		self._vNombreCamposSm = ""
		self.vNombreCamposSmCurr = ""
		self.vNombreCamposSmPrior = ""
		self.vFlagCambioInteres = ""

	@classmethod
	def from_dict(cls, data: dict):
		campo = cls()
		campo.__dict__.update(data)
		return campo

	def setNombreCampo(self, vNombreCampo: str, vNumero: int):
		self.vNombreCampo = vNombreCampo
		self.vNombreCampoUltHab = vNombreCampo[:26] + "_UH" + str(vNumero)
		self.vNombreCampoAcum = vNombreCampo[:26] + "_AC" + str(vNumero)
		# ! NOTA: se mantuvo el valor original de 25
		# ? debería reemplazarse por 24? La idea original pareciera ser mantener los nombres de tamaño 30
		self.vNombreCampoCurr = vNombreCampo[:25] + "_C_AC" + str(vNumero)
		self.vNombreCampoPrior = vNombreCampo[:25] + "_P_AC" + str(vNumero)

	@property
	def vNombreCamposSm(self):
		return self._vNombreCamposSm

	@vNombreCamposSm.setter
	def vNombreCamposSm(self, vNombreCamposSm: str):
		self._vNombreCamposSm = vNombreCamposSm

		# ! NOTA: no se esta verificando que se mantenga el limite de 30 caracteres
		if self.vNombreCamposSmPrior:
			self._vNombreCamposSm = (
				self._vNombreCamposSm + "@PRIOR@" + self.vNombreCamposSmPrior
			)

		if self.vNombreCamposSmCurr:
			self._vNombreCamposSm = (
				self._vNombreCamposSm + "@CURR@" + self.vNombreCamposSmCurr
			)

def getCampos(
		vStr: str,
		vTipoCampo: str,
		vSeparador: str,
		vInicio: str,
		vFin: str,
		vIdentacion: str,
	) -> str:
		result = ""
		vStrAux = vStr
		vCampoAux = Campo()
		vNumero = ""
		vFlagInicio = "false"  # Flag que indica cuando empieza el tipo de campo que se quiere imprimir
		vFlagImprimir = False  # Indica cuando se debe imprimir o no un campo
		# Indica si el string vStr ingresado contiene saldos de reprecio (CURR o PRIOR) lo cual requiere de procesamiento adicional
		vFlagSaldoReprecio = (
			"[SALDO_CURR]" in vStr
			or "[SALDO_PRIOR]" in vStr
			or "[SALDO_PUNT_REPRE]" in vStr
			or "[SALDO_ULT_REPRE]" in vStr
		)

		# Si vStr contiene saldos de reprecio se cuentan cuantos saldos de reprecio tiene
		vNumSaldosReprecio = 0
		if vFlagSaldoReprecio:
			for c in Campo.vListaCampos:
				if c.vFlagCambioInteres:
					vNumSaldosReprecio += 1

		# Crear iteradores para la lista de campos
		vIterator = iter(Campo.vListaCampos)
		vIterator2 = iter(Campo.vListaCampos)

		# Avanzar el segundo iterador una posición
		try:
			next(vIterator2)
		except StopIteration:
			pass

		for vCampoAux in vIterator:
			vStrAux = vStr
			vNumero = str(vCampoAux.vNumero) if vCampoAux.vNumero > 1 else ""

			if vTipoCampo == "SALDO_PUNTUAL" and vCampoAux.vTipoCampo == "SALDO_PUNTUAL":
				vStrAux = vStrAux.replace("[SALDO_PUNT]", vCampoAux.vNombreCampo)
				vStrAux = vStrAux.replace("[SALDO_ACUM]", vCampoAux.vNombreCampoAcum)
				vStrAux = vStrAux.replace("[SALDO_ULT]", vCampoAux.vNombreCampoUltHab)
				vStrAux = vStrAux.replace("[SALDO_DESC]", vCampoAux.vDescCampo)
				vStrAux = vStrAux.replace("[SALDO_CURR]", vCampoAux.vNombreCampoCurr)
				vStrAux = vStrAux.replace("[SALDO_PRIOR]", vCampoAux.vNombreCampoPrior)
				vStrAux = vStrAux.replace("[SALDO_NUM]", str(vCampoAux.vNumero))
				vStrAux = vStrAux.replace("[FLAG_REPRECIO]", vCampoAux.vFlagCambioInteres)
				vStrAux = vStrAux.replace("[SALDO_PUNT_REPRE]", vCampoAux.vNombreCampo)
				vStrAux = vStrAux.replace("[SALDO_ULT_REPRE]", vCampoAux.vNombreCampoUltHab)
				vFlagImprimir = True

				if vFlagSaldoReprecio and not vCampoAux.vFlagCambioInteres:
					vFlagImprimir = False
				if vFlagSaldoReprecio and vCampoAux.vFlagCambioInteres:
					vNumSaldosReprecio -= 1
				if vFlagInicio == "false" and vFlagImprimir:
					vFlagInicio = "true"

			elif (
				vTipoCampo == "CLAVE_PRODUCTO" and vCampoAux.vTipoCampo == "CLAVE_PRODUCTO"
			):
				vStrAux = vStrAux.replace("[CLAVE_PROD]", vCampoAux.vNombreCampo)
				vStrAux = vStrAux.replace("[CLAVE_PROD_SM]", "CLAVE_PROD" + vNumero)
				vStrAux = vStrAux.replace("[CLAVE_DESC]", vCampoAux.vDescCampo)
				vStrAux = vStrAux.replace("[CLAVE_PROD_NUM]", str(vCampoAux.vNumero))
				vFlagImprimir = True
				vFlagInicio = "true" if vFlagInicio == "false" else "NA"

			elif (
				vTipoCampo == "FECHA_REPRECIO" and vCampoAux.vTipoCampo == "FECHA_REPRECIO"
			):
				vStrAux = vStrAux.replace("[FECHA_REPRECIO]", vCampoAux.vNombreCampo)
				vStrAux = vStrAux.replace("[FECHA_REPRECIO_DESC]", vCampoAux.vDescCampo)
				vStrAux = vStrAux.replace("[FECHA_REPRECIO_NUM]", str(vCampoAux.vNumero))
				vFlagImprimir = True
				vFlagInicio = "true" if vFlagInicio == "false" else "NA"

			elif vTipoCampo == "TIPO_INTERES" and vCampoAux.vTipoCampo == "TIPO_INTERES":
				vStrAux = vStrAux.replace("[TIPO_INTERES]", vCampoAux.vNombreCampo)
				vStrAux = vStrAux.replace("[TIPO_INTERES_DESC]", vCampoAux.vDescCampo)
				vStrAux = vStrAux.replace("[TIPO_INTERES_NUM]", str(vCampoAux.vNumero))
				vFlagImprimir = True
				vFlagInicio = "true" if vFlagInicio == "false" else "NA"

			elif (
				vTipoCampo == "FECHA_PROX_REPRE"
				and vCampoAux.vTipoCampo == "FECHA_PROX_REPRE"
			):
				vStrAux = vStrAux.replace("[FECHA_PROX_REPRE]", vCampoAux.vNombreCampo)
				vStrAux = vStrAux.replace("[FECHA_PROX_REPRE_DESC]", vCampoAux.vDescCampo)
				vStrAux = vStrAux.replace("[FECHA_PROX_REPRE_NUM]", str(vCampoAux.vNumero))
				vFlagImprimir = True
				vFlagInicio = "true" if vFlagInicio == "false" else "NA"

			if vFlagImprimir:
				# Obtener el siguiente elemento del segundo iterador si existe
				try:
					next_campo = next(vIterator2)
				except StopIteration:
					next_campo = None

				if vFlagInicio == "true" and next_campo is not None:
					vFlagInicio = "NA"
					if (
						next_campo.vTipoCampo == vCampoAux.vTipoCampo
						and not vFlagSaldoReprecio
					) or (vFlagSaldoReprecio and vNumSaldosReprecio > 0):
						if vCampoAux.vNombreCampo:
							result += vInicio + vStrAux + vSeparador + "\n"
					else:
						if vCampoAux.vNombreCampo:
							result += vInicio + vStrAux + vFin
							break
				elif vFlagInicio == "true" and next_campo is None:
					vFlagInicio = "NA"
					if vCampoAux.vNombreCampo:
						result += vInicio + vStrAux + vFin
						break
				elif vFlagInicio != "true" and next_campo is None:
					if vCampoAux.vNombreCampo:
						result += vIdentacion + vStrAux + vFin
						break
				elif vFlagInicio != "true" and next_campo is not None:
					if (
						next_campo.vTipoCampo == vCampoAux.vTipoCampo
						and not vFlagSaldoReprecio
					) or (vFlagSaldoReprecio and vNumSaldosReprecio > 0):
						if vCampoAux.vNombreCampo:
							result += vIdentacion + vStrAux + vSeparador + "\n"
					else:
						if vCampoAux.vNombreCampo:
							result += vIdentacion + vStrAux + vFin
							break
			else:
				# Avanzar el segundo iterador si no se imprime
				try:
					next(vIterator2)
				except StopIteration:
					pass

			vFlagImprimir = False

		return result
