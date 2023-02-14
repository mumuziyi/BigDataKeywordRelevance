package uk.ac.gla.dcs.bigdata;

import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        String str = "colleg danni coal jarrett boykin perfect 1 2 punch virginia tech mark giannotto span class datelin new orlean span href http www washingtonpost com blog hoki journal titl www washingtonpost com virginia tech offens coach ask prolif receiv duo school histori came inevit first road game 2008 north carolina come \n" +
                "midwai first quarter virginia tech call timeout row freshmen href http stat washingtonpost com cfb player asp id team 16 titl stat washingtonpost com jarrett boykin href http stat washingtonpost com cfb player asp id team 16 titl stat washingtonpost com danni coal couldn line right big ey look kevin sherman posit coach recent \n" +
                "now boykin coal onli tuesdai sugar bowl remain leav virginia tech major school record wide receiv ve taken stanc \n" +
                "still don think macho harri game line wrong boykin coal sat next nod agreement \n" +
                "just add list slight senior overcom \n" +
                "boykin href http stat washingtonpost com cfb teamstat asp team 16 report stat titl stat washingtonpost com team lead receiv past season us hand need size xxxl glove knack maneuv oppos cornerback air set singl season school record recept year 57 will end career catch 180 yard 2 854 hoki receiv \n" +
                "coal episcop high graduat lead virginia tech 785 receiv yard year right boykin school record book team start punter end season coach frank beamer frequent marvel danni just open \n" +
                "warrant honor mention acc statu year snub quarterback href http stat washingtonpost com cfb player asp id team 16 titl stat washingtonpost com logan thoma made extrem upset left beamer wonder media member who particip vote \n" +
                "retrospect boykin recogn lack notorieti partli due virginia tech offens philosophi hoki known rush attack year run back href http stat washingtonpost com cfb player asp id team 16 titl stat washingtonpost com david wilson earn acc player year honor year thoma set multipl record first year quarterback \n" +
                "just held back be abl show boykin just south carolina wide receiv href http stat washingtonpost com cfb player asp id team 70 titl stat washingtonpost com alshon jeffrei oklahoma state wide receiv href http stat washingtonpost com cfb player asp id team 25 titl stat washingtonpost com justin blackmon feel like great athlet time right \n" +
                "great plai wide receiv here onc throw ball opportun big chunk yardag can go catch 100 ball 1 500 yard 22 touchdown \n" +
                "issu sort attent grab person pedigre associ big time wide receiv dai \n" +
                "coal graduat degre financ name acc top scholar athlet year speak measur tone reminisc ceo join facebook twitter boykin quiet team facil beamer doesn notic make catch practic field game \n" +
                "come high school coal bare recruit show camp blacksburg summer onli scholarship offer vmi father head strength condit coal still joke spent redshirt year 2007 scout team virginia tech wide receiv futur nfl wideout eddi royal andr davi josh morgan thought walk prefer just fly radar \n" +
                "accomplish haven unnot now clock tick career quarterback coach mike cain thoma comfort level record set first year center direct reflect boykin coal onli gonna run right rout right time know gonna catch ball \n" +
                "year line creat special bond plai acc championship game year \n" +
                "boykin suppos deliv pregam speech retic public speak afraid stutter taken serious ask coal take place \n" +
                "ve struggl mine coal gui know can count year now just know can count ll know look back part tech experi go ";
        TextPreProcessor processor = new TextPreProcessor();
        System.out.println(str.split(" ").length);
    }
}
