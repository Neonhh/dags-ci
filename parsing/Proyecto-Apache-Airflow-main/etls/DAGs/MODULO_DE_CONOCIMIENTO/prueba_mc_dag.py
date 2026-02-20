from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.base import BaseSensorOperator
from airflow.operators.email import EmailOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime
import time

### FUNCIONES DE CADA TAREA ####

def IT_SM_STG_GL_DATA(**kwargs):
    hook = PostgresHook(postgres_conn_id='mi_conexion')

    # Declaracion de variables saldos medios (BeanShell)
    sql_query_deftxt = '''import java.lang.*;
import java.util.ArrayList;
import java.util.List;

//Clase Campo representa un campo y los atributos del mismo
public class Campo {
    private String vNombreCampo = "";
    private String vTipoCampo = "";
    private Integer vNumero = 0;
    private String vDescCampo = "";
    private String vNombreCampoUltHab = "";
    private String vNombreCampoAcum = "";
    private String vNombreCampoCurr = "";
    private String vNombreCampoPrior = "";
    private String vNombreCamposSm = "";
    private String vNombreCamposSmCurr = "";
    private String vNombreCamposSmPrior = "";
    private String vFlagCambioInteres = "";
    
    public void SetNombreCampo (String vNombreCampo) {this.vNombreCampo = vNombreCampo;}
    public void SetNombreCampo (String vNombreCampo, int vNumero) {
        this.vNombreCampo = vNombreCampo;
        this.vNombreCampoUltHab = vNombreCampo.length() > 26  ? vNombreCampo.substring(0,26) + "_UH" + vNumero : vNombreCampo + "_UH" + vNumero;
        this.vNombreCampoAcum = vNombreCampo.length() > 26  ? vNombreCampo.substring(0,26) + "_AC" + vNumero : vNombreCampo + "_AC" + vNumero;
        this.vNombreCampoCurr = vNombreCampo.length() > 25  ? vNombreCampo.substring(0,25) + "_C_AC" + vNumero : vNombreCampo + "_C_AC" + vNumero;
        this.vNombreCampoPrior = vNombreCampo.length() > 25  ? vNombreCampo.substring(0,25) + "_P_AC" + vNumero : vNombreCampo + "_P_AC" + vNumero;
    }
    public String GetNombreCampo () {return this.vNombreCampo;}
    public void SetTipoCampo (String vTipoCampo) {this.vTipoCampo = vTipoCampo;}
    public String GetTipoCampo () {return this.vTipoCampo;}
    public void SetNumero (Integer vNumero) {this.vNumero = vNumero;}
    public Integer GetNumero () {return this.vNumero;}
    public void SetDescCampo (String vDescCampo) {this.vDescCampo = vDescCampo;}
    public String GetDescCampo () {return this.vDescCampo;}
    public void SetNombreCampoUltHab (String vNombreCampoUltHab) {this.vNombreCampoUltHab = vNombreCampoUltHab;}
    public String GetNombreCampoUltHab () {return this.vNombreCampoUltHab;}
    public void SetNombreCampoAcum (String vNombreCampoAcum) {this.vNombreCampoAcum = vNombreCampoAcum;}
    public String GetNombreCampoAcum () {return this.vNombreCampoAcum;}
    public void SetNombreCampoCurr (String vNombreCampoCurr) {this.vNombreCampoCurr = vNombreCampoCurr;}
    public String GetNombreCampoCurr () {return this.vNombreCampoCurr;}
    public void SetNombreCampoPrior (String vNombreCampoPrior) {this.vNombreCampoPrior = vNombreCampoPrior;}
    public String GetNombreCampoPrior () {return this.vNombreCampoPrior;}
    public void SetNombreCamposSm (String vNombreCamposSm) {
        this.vNombreCamposSm = vNombreCamposSm;
        this.vNombreCamposSm = this.vNombreCamposSmPrior.length() > 0 ? this.vNombreCamposSm + "@PRIOR@" + this.vNombreCamposSmPrior : this.vNombreCamposSm;
        this.vNombreCamposSm = this.vNombreCamposSmCurr.length() > 0 ? this.vNombreCamposSm + "@CURR@" + this.vNombreCamposSmCurr : this.vNombreCamposSm;
    }
    public String GetNombreCamposSm () {return this.vNombreCamposSm;}
    public void SetNombreCamposSmCurr (String vNombreCamposSmCurr) {this.vNombreCamposSmCurr = vNombreCamposSmCurr;}
    public String GetNombreCamposSmCurr () {return this.vNombreCamposSmCurr;}
    public void SetNombreCamposSmPrior (String vNombreCamposSmPrior) {this.vNombreCamposSmPrior = vNombreCamposSmPrior;}
    public String GetNombreCamposSmPrior () {return this.vNombreCamposSmPrior;}
    public void SetFlagCambioInteres (String vFlagCambioInteres) {this.vFlagCambioInteres = vFlagCambioInteres;}
    public String GetFlagCambioInteres () {return this.vFlagCambioInteres;}
}

public static void getCampos(String vStr, String vTipoCampo, String vSeparador, String vInicio, String vFin, String vIdentacion){
    String vStrAux = vStr;
    Campo vCampoAux;
    String vNumero = "";
    String vFlagInicio = "false"; //Flag que indica cuando empieza el tipo de campo que se quiere imprimir
    boolean vFlagImprimir = false; //Indica cuando se debe imprimir o no un campo
    //Indica si el string vStr ingresado contiene saldos de reprecio (CURR o PRIOR) lo cual requiere de procesamiento adicional
    boolean vFlagSaldoReprecio = vStr.contains("[SALDO_CURR]") || vStr.contains("[SALDO_PRIOR]") || vStr.contains("[SALDO_PUNT_REPRE]") || vStr.contains("[SALDO_ULT_REPRE]") ? true: false; 
    int vNumSaldosReprecio = 0;
    
    //Si vStr contiene saldos de reprecio se cuentan cuantos saldos de reprecio tiene
    Iterator vIterator = vListaCampos.iterator();
    if (vFlagSaldoReprecio) {
      while (vIterator.hasNext()) {
        vCampoAux = vIterator.next();
        vNumSaldosReprecio = vCampoAux.GetFlagCambioInteres().length() > 0 ? vNumSaldosReprecio + 1: vNumSaldosReprecio;
      }
    }	
    
    vIterator = vListaCampos.iterator();
    Iterator vIterator2 = vListaCampos.iterator();
    if (vIterator2.hasNext()) {vIterator2.next();}
    while (vIterator.hasNext()) {
        vCampoAux = vIterator.next();
        vStrAux = vStr;
        vNumero = vCampoAux.GetNumero() > 1  ? "" + vCampoAux.GetNumero() : "";
        
        if (vTipoCampo.equals("SALDO_PUNTUAL") && vCampoAux.GetTipoCampo().equals("SALDO_PUNTUAL")) {
            vStrAux = vStrAux.replace("[SALDO_PUNT]", vCampoAux.GetNombreCampo());
            vStrAux = vStrAux.replace("[SALDO_ACUM]", vCampoAux.GetNombreCampoAcum());
            vStrAux = vStrAux.replace("[SALDO_ULT]", vCampoAux.GetNombreCampoUltHab());
            vStrAux = vStrAux.replace("[SALDO_DESC]", vCampoAux.GetDescCampo());
            vStrAux = vStrAux.replace("[SALDO_CURR]", vCampoAux.GetNombreCampoCurr());
            vStrAux = vStrAux.replace("[SALDO_PRIOR]", vCampoAux.GetNombreCampoPrior());
            vStrAux = vStrAux.replace("[SALDO_NUM]", "" + vCampoAux.GetNumero());
            vStrAux = vStrAux.replace("[FLAG_REPRECIO]", vCampoAux.GetFlagCambioInteres());
            vStrAux = vStrAux.replace("[SALDO_PUNT_REPRE]", vCampoAux.GetNombreCampo());
            vStrAux = vStrAux.replace("[SALDO_ULT_REPRE]", vCampoAux.GetNombreCampoUltHab());
            vFlagImprimir = true;
            if (vFlagSaldoReprecio && vCampoAux.GetFlagCambioInteres().length() == 0) {vFlagImprimir = false;}
            if (vFlagSaldoReprecio && vCampoAux.GetFlagCambioInteres().length() > 0) {vNumSaldosReprecio = vNumSaldosReprecio -1;}
            if (vFlagInicio.equals("false") && vFlagImprimir) {vFlagInicio = "true";}
        } 
        else if(vTipoCampo.equals("CLAVE_PRODUCTO") && vCampoAux.GetTipoCampo().equals("CLAVE_PRODUCTO")) {
            vStrAux = vStrAux.replace("[CLAVE_PROD]", vCampoAux.GetNombreCampo());
            vStrAux = vStrAux.replace("[CLAVE_PROD_SM]", "CLAVE_PROD" + vNumero);
            vStrAux = vStrAux.replace("[CLAVE_DESC]", vCampoAux.GetDescCampo());
            vStrAux = vStrAux.replace("[CLAVE_PROD_NUM]", "" + vCampoAux.GetNumero());
  	          vFlagImprimir = true;
            vFlagInicio = vFlagInicio.equals("false") ? "true": "NA";
        }
        else if(vTipoCampo.equals("FECHA_REPRECIO") && vCampoAux.GetTipoCampo().equals("FECHA_REPRECIO")) {
            vStrAux = vStrAux.replace("[FECHA_REPRECIO]", vCampoAux.GetNombreCampo());
            vStrAux = vStrAux.replace("[FECHA_REPRECIO_DESC]", vCampoAux.GetDescCampo());
            vStrAux = vStrAux.replace("[FECHA_REPRECIO_NUM]", "" + vCampoAux.GetNumero());
            vFlagImprimir = true;
            vFlagInicio = vFlagInicio.equals("false") ? "true": "NA";
        }
        else if(vTipoCampo.equals("TIPO_INTERES") && vCampoAux.GetTipoCampo().equals("TIPO_INTERES")) {
            vStrAux = vStrAux.replace("[TIPO_INTERES]", vCampoAux.GetNombreCampo());
            vStrAux = vStrAux.replace("[TIPO_INTERES_DESC]", vCampoAux.GetDescCampo());
            vStrAux = vStrAux.replace("[TIPO_INTERES_NUM]", "" + vCampoAux.GetNumero());
            vFlagImprimir = true;
            vFlagInicio = vFlagInicio.equals("false") ? "true": "NA";
        }
        else if(vTipoCampo.equals("FECHA_PROX_REPRE") && vCampoAux.GetTipoCampo().equals("FECHA_PROX_REPRE")) {
            vStrAux = vStrAux.replace("[FECHA_PROX_REPRE]", vCampoAux.GetNombreCampo());
            vStrAux = vStrAux.replace("[FECHA_PROX_REPRE_DESC]", vCampoAux.GetDescCampo());
            vStrAux = vStrAux.replace("[FECHA_PROX_REPRE_NUM]", "" + vCampoAux.GetNumero());
            vFlagImprimir = true;
            vFlagInicio = vFlagInicio.equals("false") ? "true": "NA";
        }
		
        if (vFlagImprimir) {		
            if (vFlagInicio.equals("true") && vIterator.hasNext()) {
                vFlagInicio = "NA";
                if ((vIterator2.next().GetTipoCampo().equals(vCampoAux.GetTipoCampo()) && !vFlagSaldoReprecio) || (vFlagSaldoReprecio && vNumSaldosReprecio > 0)) {
                  if (vCampoAux.GetNombreCampo().length() > 0) {out.println(vInicio + vStrAux + vSeparador);}
                } 
                else {
                  if (vCampoAux.GetNombreCampo().length() > 0) {out.print(vInicio + vStrAux + vFin); break;}
                }
            }
            else if (vFlagInicio.equals("true") && !vIterator.hasNext()) {
                vFlagInicio = "NA";
                if (vCampoAux.GetNombreCampo().length() > 0) {out.print(vInicio + vStrAux + vFin); break;}
            }
            else if (!vFlagInicio.equals("true") && !vIterator.hasNext()) {
                if (vCampoAux.GetNombreCampo().length() > 0) {out.print(vIdentacion + vStrAux + vFin); break;}
            }
            else if (!vFlagInicio.equals("true") && vIterator.hasNext()) {
                if ((vIterator2.next().GetTipoCampo().equals(vCampoAux.GetTipoCampo()) && !vFlagSaldoReprecio) || (vFlagSaldoReprecio && vNumSaldosReprecio > 0)) {
                  if (vCampoAux.GetNombreCampo().length() > 0) {out.println(vIdentacion + vStrAux + vSeparador);}
                } 
                else {
                  if (vCampoAux.GetNombreCampo().length() > 0) {out.print(vIdentacion + vStrAux + vFin); break;}
                }
            }		
        }
        else { if (vIterator2.hasNext()) {vIterator2.next();}}
        vFlagImprimir = false;
    }
}

public void consultarCampos(ArrayList vListaCampos) {
    Campo vCampoAux;
   vCampoAux = new Campo(); 
vCampoAux.SetNumero(1); 
vCampoAux.SetTipoCampo("CLAVE_PRODUCTO"); 
vCampoAux.SetNombreCampo("V_GL_CODE", 1);
vCampoAux.SetDescCampo("VARCHAR2(20)");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(2); 
vCampoAux.SetTipoCampo("CLAVE_PRODUCTO"); 
vCampoAux.SetNombreCampo("V_ORG_UNIT_CODE", 2);
vCampoAux.SetDescCampo("VARCHAR2(40)");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(3); 
vCampoAux.SetTipoCampo("CLAVE_PRODUCTO"); 
vCampoAux.SetNombreCampo("", 3);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(4); 
vCampoAux.SetTipoCampo("CLAVE_PRODUCTO"); 
vCampoAux.SetNombreCampo("", 4);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(5); 
vCampoAux.SetTipoCampo("CLAVE_PRODUCTO"); 
vCampoAux.SetNombreCampo("", 5);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}

    vCampoAux = new Campo(); 
vCampoAux.SetNumero(1); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("N_AMOUNT_LCY", 1);
vCampoAux.SetDescCampo("NUMBER(22,3)");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(2); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("N_AMOUNT_ACY", 2);
vCampoAux.SetDescCampo("NUMBER(22,3)");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(3); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 3);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(4); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 4);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(5); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 5);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(6); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 6);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(7); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 7);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(8); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 8);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(9); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 9);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(10); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 10);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(11); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 11);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(12); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 12);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(13); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 13);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(14); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 14);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(15); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 15);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(16); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 16);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(17); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 17);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(18); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 18);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(19); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 19);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(20); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 20);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
vCampoAux = new Campo(); 
vCampoAux.SetNumero(21); 
vCampoAux.SetTipoCampo("SALDO_PUNTUAL"); 
vCampoAux.SetFlagCambioInteres(""); 
vCampoAux.SetNombreCampo("", 21);
vCampoAux.SetDescCampo("");
if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
        
    //Otros Campos
    //Fecha Utimo Reprecio
    String vFechaUltRe = "";
    vCampoAux = new Campo();
    vCampoAux.SetNumero(1);
    vCampoAux.SetTipoCampo("FECHA_REPRECIO");
    vCampoAux.SetNombreCampo(vFechaUltRe);
    vCampoAux.SetDescCampo("DATE");
    if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
	
    //Fecha Proximo Reprecio
    String vFechaUltRe = "";
    vCampoAux = new Campo();
    vCampoAux.SetNumero(1);
    vCampoAux.SetTipoCampo("FECHA_PROX_REPRE");
    vCampoAux.SetNombreCampo(vFechaUltRe);
    vCampoAux.SetDescCampo("DATE");
    if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
	
    //Tipo de interes
    String vFechaUltRe = "";
    vCampoAux = new Campo();
    vCampoAux.SetNumero(1);
    vCampoAux.SetTipoCampo("TIPO_INTERES");
    vCampoAux.SetNombreCampo(vFechaUltRe);
    vCampoAux.SetDescCampo("");
    if (vCampoAux.GetNombreCampo().length() > 0) {vListaCampos.add(vCampoAux);}
}

//Nombre de la tabla STG, se limpia en caso de que tenga el nombre del esquema o del dblink
String vNombreTablaSTG = "STG_GL_DATA";

//Nombre de la tabla de saldos puntuales acumulados, se crea una por cada producto
String vNombreTablaSM = vNombreTablaSTG.length() > 27  ? "SM_" + vNombreTablaSTG.substring(0,27)  : "SM_" + vNombreTablaSTG;

//Fecha para el cual se va crear la tabla temporal de saldos puntuales del dÃ­a, se toma los dias para diferenciar la distintas tablas de saldos puntuales diarios
String vFechaEjecutar = "{vStgSmFechaProcesar}";

//Nombre de la tabla temporal de saldos puntuales del dia extraidos de ods
String vNombreTablaAuxSMExt = vNombreTablaSTG.length() > 24 ? "SM" + vFechaEjecutar.substring(3,5) + "E$" + vNombreTablaSTG.substring(0,24)  : "SM" + vFechaEjecutar.substring(3,5) + "E$" + vNombreTablaSTG;

//Nombre de la tabla temporal de saldos puntuales del dia divido en lotes
String vNombreTablaAuxSM = vNombreTablaSTG.length() > 23 ? "SM" + vFechaEjecutar.substring(3,5) + "$" + vNombreTablaSTG.substring(0,23)  : "SM" + vFechaEjecutar.substring(3,5) + "$" + vNombreTablaSTG;

//Nombre de la tabla temporal que almacena el nÃºmero de lotes generado
String vNombreTablaLotesSM = vNombreTablaSTG.length() > 24  ? "SM" + vFechaEjecutar.substring(3,5) + "L$" + vNombreTablaSTG.substring(0,24)  : "SM" + vFechaEjecutar.substring(3,5) + "L$" + vNombreTablaSTG;

//Nombre del indice de tabla saldos puntuales acumulados
String vNombreIndiceSM = vNombreTablaSM.length() > 24 ? "IDX01_" + vNombreTablaSM.substring(0,24)  : "IDX01_" + vNombreTablaSM;

//Nombre del indice de tabla temporal saldos puntuales dia
String vNombreIndiceAuxSM = vNombreTablaAuxSM.length() > 24 ? "IDX01_" + vNombreTablaAuxSM.substring(0,24)  : "IDX01_" + vNombreTablaAuxSM;

ArrayList vListaCampos = new ArrayList(); //Lista que contiene los campos de saldos puntuales, clave producto y otros campos que no sean saldos medios
consultarCampos(vListaCampos); 

;'''
    hook.run(sql_query_deftxt)


###### DEFINICION DEL DAG ###### 

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

dag = DAG(dag_id='PKG_CON_SM_EXTRACTODS_STG_GL_DATA',
          default_args=default_args,
          schedule_interval=None, # Aqui se programa cada cuanto ejecutar el DAG
          catchup=False)

IT_SM_STG_GL_DATA_task = PythonOperator(
    task_id='IT_SM_STG_GL_DATA_task',
    python_callable=IT_SM_STG_GL_DATA,
    provide_context=True,
    dag=dag
)

###### SECUENCIA DE EJECUCION ######
IT_SM_STG_GL_DATA_task
