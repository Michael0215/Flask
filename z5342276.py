import datetime,json,re
import requests
from datetime import datetime,timedelta
from flask_sqlalchemy import SQLAlchemy
from flask import Flask, request, send_file, make_response
from flask_restx import Api, Resource, reqparse, fields
import matplotlib.pyplot as plt


TVMaze = Flask(__name__)
TVMaze.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
api = Api(TVMaze)
db = SQLAlchemy(TVMaze)
TVMaze.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///z5342276.db'

def showInfo(actor_id):
    showsList = []
    crewcredits_url = 'https://api.tvmaze.com/people/' + str(actor_id) + '/castcredits'
    crewcreditsURL = requests.get(crewcredits_url)
    if crewcreditsURL.status_code == 200:
        shows_list = json.loads(crewcreditsURL.text)
        for show in shows_list:
            show_href = show['_links']['show']['href']
            show_url = requests.get(show_href)
            if show_url.status_code == 200:
                show = json.loads(show_url.text)
                showsList.append(show['name'])
        showsList = ','.join(showsList)
    return showsList

def actorInfo(url):
    actorsURL = requests.get(url)
    if actorsURL.status_code == 200:
        if json.loads(actorsURL.text) == []:
            message = 'This actor doesn\'t exist in TVMaze website!'
            return message
        else:
            actor_info = json.loads(actorsURL.text)[0]['person']
            actor_id = actor_info['id']
            actor_url = actor_info['url']
            actor_showList = showInfo(actor_id)
            actorsInfo = []
            if actor_showList == '':
                actor_showList = None
            name = actor_info['name']
            actorsInfo.append(name)
            gender = actor_info['gender']
            actorsInfo.append(gender)
            if actor_info['country']:
                country = actor_info['country']['name']
                actorsInfo.append(country)
            else:
                country = None
                actorsInfo.append(country)
            actorsInfo.append(actor_url)
            birthday = actor_info['birthday']
            if birthday:
                birthday1 = datetime.strptime(birthday,'%Y-%m-%d')
                actorsInfo.append(birthday1)
            else:
                actorsInfo.append(birthday)
            deathday = actor_info['deathday']
            if deathday:
                deathday1 = datetime.strptime(deathday, '%Y-%m-%d')
                actorsInfo.append(deathday1)
            else:
                actorsInfo.append(deathday)
            if birthday:
                birthday2 = datetime.strptime(birthday, '%Y-%m-%d')
                if not deathday:
                    age = int(datetime.now().strftime('%Y')) - int(birthday2.strftime('%Y'))
                    actorsInfo.append(age)
                else:
                    deathday2 = datetime.strptime(deathday, '%Y-%m-%d')
                    age = int(deathday2.strftime('%Y')) - int(birthday2.strftime('%Y'))
                    actorsInfo.append(age)
            else:
                age = None
                actorsInfo.append(age)
            actorsInfo.append(actor_showList)
            return actorsInfo

class ActorsDB(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), unique=True, nullable=False)
    gender = db.Column(db.String(10), nullable=True)
    country = db.Column(db.String(50), nullable=True)
    URL = db.Column(db.Text, unique=True, nullable=False)
    birthday = db.Column(db.DateTime, nullable=True)
    deathday = db.Column(db.DateTime, nullable=True)
    age = db.Column(db.Integer, nullable=True)
    show = db.Column(db.Text, nullable=True)
    last_update = db.Column(db.DateTime, default=datetime.now())

actor_name = reqparse.RequestParser()
actor_name.add_argument('name', type=str)

actor_list = reqparse.RequestParser()
actor_list.add_argument('page', type =int, default=1)
actor_list.add_argument('size', type=int, default=10)
actor_list.add_argument('order', type=str, default='+id')
actor_list.add_argument('filter', type=str, default='id,name')

visualize = reqparse.RequestParser()
visualize.add_argument('format', type=str)
visualize.add_argument('by', type=str)


@api.route('/actors')
class ActoFromHostURL(Resource):

    @api.expect(actor_name)
    def post(self):

        actorsInfo = actorInfo('https://api.tvmaze.com/search/people?q=' + re.sub('[^A-Za-z]', ' ', request.args.get('name')))
        if actorsInfo == 'This actor doesn\'t exist in TVMaze website!':
            return 'This actor doesn\'t exist in TVMaze website!', 404
        else:
            #all infos here
            actor = ActorsDB.query.all()
            name_list = []
            for i in range(len(actor)):
                name_list.append(actor[i].name)
            if actorsInfo[0] in name_list:
                return 'This actor is already in ActorDB, it can\'t be imported again!', 403
            else:
                actors = ActorsDB(name=actorsInfo[0], gender=actorsInfo[1], country=actorsInfo[2], URL=actorsInfo[3],
                                  birthday=actorsInfo[4], deathday=actorsInfo[5], age=actorsInfo[6], show=actorsInfo[7])
                db.session.add(actors)
                db.session.commit()
                actor = ActorsDB.query.filter_by(name=actorsInfo[0]).first()
                return {'id': actor.id,
                        'last-update': actor.last_update.strftime('%Y-%m-%d-%H:%M:%S'),
                        '_links': {
                            'self': {
                                'href': 'http://127.0.0.1:5000/actors/' + str(actor.id)
                            }
                        }
                        }, 200

    @api.expect(actor_list)
    def get(self):
        page = request.args.get('page')
        size = request.args.get('size')
        order = request.args.get('order').split(',')
        filter = request.args.get('filter').split(',')
        filterList = []
        db_column_name = ['id','name','gender','birthday','deathday','country','show','last_update']
        new_order = []
        mergeList = []
        for column in order:
            new_order.append(column[1:])
        mergeList = new_order + filter
        flag = 0
        for column in mergeList:
            if column not in db_column_name:
                flag = 1
        if flag == 1: # no column named like this
            return 'Some column names can not be found!', 404
        else:
            for column in filter:
                filterList.append(eval('ActorsDB.'+ column))
            orderList = []
            for column in order:
                if column[0] == '+' or column[0] == ' ':
                    orderList.append(eval('ActorsDB.'+ column[1:] + '.asc()'))
                else:
                    orderList.append(eval('ActorsDB.'+ column[1:] + '.desc()'))
            actorList = db.session.query(*filterList).order_by(*orderList).all()
            totalNum = len(actorList)
            start = (int(page)-1)*int(size)+1
            end = start+int(size)-1
            orderJoin = ','.join(order)
            filterJoin = ','.join(filter)
            json = {
                'page': page,
                'page-size': size,
                'actors': [],
                '_links': {
                    'self': {
                        'href': 'http://127.0.0.1:5000/actors?order=' + orderJoin + '&page=' + page + '&size=' + size + '&filter=' + filterJoin
                    },
                    'next': {
                        'href': 'http://127.0.0.1:5000/actors?order=' + orderJoin + '&page=' + str(
                            int(page) + 1) + '&size=' + size + '&filter=' + filterJoin
                    },
                }
            }
            time =  ['birthday', 'deathday', 'last_update']
            if start > totalNum:
                json['actors'] = []
            elif start <= totalNum and end > totalNum:
                actors_dict_list=[]
                actors_dict={}
                for i in range(start, totalNum+1):
                    for j in range(len(filter)):
                        evaluate = eval('actorList[' + str(i - 1) + '].' + filter[j])
                        if filter[j] in time and evaluate:
                            evaluate = evaluate.strftime('%Y-%m-%d-%H:%M:%S')
                        actors_dict[filter[j]] = evaluate
                    actors_dict_list.append(actors_dict)
                    actors_dict={}
                json['actors'] = actors_dict_list
            elif end <= totalNum:
                actors_dict_list=[]
                actors_dict={}
                for i in range(start, end+1):
                    for j in range(len(filter)):
                        evaluate = eval('actorList[' + str(i - 1) + '].' + filter[j])
                        if filter[j] in time and evaluate:
                            evaluate = evaluate.strftime('%Y-%m-%d-%H:%M:%S')
                        actors_dict[filter[j]] = evaluate
                    actors_dict_list.append(actors_dict)
                    actors_dict={}
                json['actors'] = actors_dict_list
            return json, 200

# retrieved from: https://flask-restplus.readthedocs.io/en/stable/example.html
Json = api.model('json', {'actor\'s attribute': fields.String(required=True, description='values for actor to be changed')})

@api.route('/actors/<int:actors_id>')
class ActorModify(Resource):
    def get(self, actors_id):
        actor = ActorsDB.query.all()
        id_list = []
        for i in range(len(actor)):
            id_list.append(actor[i].id)
        if actors_id not in id_list:
            return 'This actor can not be got!', 404
        else:
            actor = ActorsDB.query.filter_by(id=actors_id).first()
            if actor.deathday:
                deathday = actor.deathday.strftime('%d-%m-%Y')
            else:
                deathday = None
            if actor.birthday:
                birthday = actor.birthday.strftime('%d-%m-%Y')
            else:
                birthday = None
            if actor.show == None:
                show = actor.show
            else:
                show = actor.show.split(',')
            json = {'id': actor.id,
                    'last_update': actor.last_update.strftime('%Y-%m-%d-%H:%M:%S'),
                    'name': actor.name,
                    'country': actor.country,
                    'birthday': birthday,
                    'deathday': deathday,
                    'shows': show,
                    '_links': {
                        'self': {
                            'href': 'http://127.0.0.1:5000/actors/' + str(actor.id)
                        }
                    }
                    }

            previous = ActorsDB.query.order_by(ActorsDB.id.desc()).filter(actor.id > ActorsDB.id).first()
            if previous:
                json['_links']['previous'] = {'href': 'http://127.0.0.1:5000/actors/' + str(previous.id)}

            next = ActorsDB.query.order_by(ActorsDB.id.asc()).filter(actor.id < ActorsDB.id).first()
            if next:
                json['_links']['next'] = {'href': 'http://127.0.0.1:5000/actors/' + str(next.id)}
            return json, 200

    def delete(self, actors_id):
        actor = ActorsDB.query.all()
        id_list = []
        for i in range(len(actor)):
            id_list.append(actor[i].id)
        if actors_id not in id_list:
            return 'This actor doesn\'t exist, and can\'t be deleted!', 403
        else:
            actor = ActorsDB.query.filter_by(id=actors_id).first()
            db.session.delete(actor)
            db.session.commit()
            return {'message': 'The actor with id ' + str(actor.id) + ' was removed from the database!',
                    'id': actor.id
                   }, 200

    @api.expect(Json)
    def patch(self, actors_id):
        actor = ActorsDB.query.all()
        id_list = []
        for i in range(len(actor)):
            id_list.append(actor[i].id)
        if actors_id not in id_list:
            return 'This actor doesn\'t exist, and his or her information can\'t be updated!', 404
        else:
            actor = ActorsDB.query.filter_by(id=actors_id).first()
            json_dict = json.loads(json.dumps(request.get_json()))

            key_list = ['name', 'gender', 'country', 'URL', 'birthday', 'deathday', 'show']
            for key in json_dict.keys():
                if key not in key_list:
                    return 'this attribute is not in ActorsDB!', 403
                else:
                    if json_dict[key] and key in ['birthday', 'deathday']:
                        json_dict[key] = datetime.strptime(json_dict[key], '%d-%m-%Y')
                    if eval('actor.'+str(key)) != json_dict[key]:
                        setattr(actor, key, json_dict[key])
                        if key == 'birthday' and key != None:
                            if not actor.deathday:
                                age_update = int(datetime.now().strftime('%Y')) - int(actor.birthday.strftime('%Y'))
                            else:

                                age_update = int(actor.deaththday.strftime('%Y')) - int(actor.birthday.strftime('%Y'))
                            setattr(actor, 'age', age_update)
                        actor.last_update = datetime.now()


            db.session.commit()
            return {'id': actor.id,
                    'last_update': actor.last_update.strftime('%Y-%m-%d-%H:%M:%S'),
                    '_links': {
                        'self': {
                            'href': 'http://127.0.0.1:5000/actors/' + str(actor.id)
                        }
                    }
                    }, 200
#http://127.0.0.1:5000//actors/statistics?format=image&by=country,birthday,gender
@api.route('/actors/statistics')
class visualize_stats(Resource):
    @api.expect(visualize)
    def get(self):
        format = request.args.get('format')
        byList = request.args.get('by').split(',')
        legal_input = ['country', 'birthday', 'gender', 'life_status']
        flag = 0
        for data in byList:
            if data not in legal_input:
                flag = 1
                break
        if flag == 1:
            return 'illegal input of statistics!', 403
        else:
            actor = ActorsDB.query.all()
            total_updated = ActorsDB.query.filter((datetime.now()-timedelta(hours=24)) <= ActorsDB.last_update).all()
            json = {'total':len(actor),
                    'total-updated': len(total_updated)
                    }
            ageList = []
            country_dict = {}

            plt.style.use('seaborn')
            if 'country' in byList:
                total_country = ActorsDB.query.filter(ActorsDB.country!=None).all()
                count_on_country = db.session.query(ActorsDB.country, db.func.count(ActorsDB.name).label('count_on_country')).filter(ActorsDB.country!=None).group_by(ActorsDB.country).all()
                for i in range(len(count_on_country)):
                    country_dict[count_on_country[i].country] = round(count_on_country[i].count_on_country/len(total_country),2)
                json['by-country'] = country_dict
                country_slice = []
                country_label = []
                for key in country_dict.keys():
                    country_label.append(key)
                    country_slice.append(country_dict[key])
                country_slice = [100*percentage for percentage in country_slice]
                explode = []
                for i in range(len(country_slice)):
                    if country_slice[i]==max(country_slice):
                        explode.append(0.1)
                    else:
                        explode.append(0)
                plt.subplot(411)
                plt.pie(country_slice, labels=country_label, explode = explode, startangle=90, autopct='%1.2f%%',wedgeprops={'edgecolor': 'black'},textprops={'fontsize': 7})
                plt.title('Ratio of Country', fontsize=10)



            ageNameList = []
            if 'birthday' in byList:
                for i in range(len(actor)):
                    if actor[i].age:
                        ageNameList.append(actor[i].name)
                        ageList.append(actor[i].age)
                json['by-birthday'] = { 'max age': max(ageList),
                                        'average age': round(sum(ageList)/len(ageList),2),
                                        'min age': min(ageList)
                                       }
                plt.subplot(412)
                plt.barh(ageNameList, ageList)
                for i, value in enumerate(ageList):
                    plt.text(value, i, str(value), color='blue')
                plt.ylabel('Actor\'s Name')
                plt.xlabel('Age')
                plt.title('Actor\'s Age Distribution', fontsize=10)


            gender_dict = {}
            if 'gender' in byList:
                total_gender = ActorsDB.query.filter(ActorsDB.gender != None).all()
                count_on_gender = db.session.query(ActorsDB.gender,db.func.count(ActorsDB.name).label('count_on_gender')).filter(ActorsDB.gender != None).group_by(ActorsDB.gender).all()
                for i in range(len(count_on_gender)):
                    gender_dict[count_on_gender[i].gender] = round(count_on_gender[i].count_on_gender / len(total_gender), 2)
                json['by-gender'] = gender_dict
                gender_slice = []
                gender_label = []
                for key in gender_dict.keys():
                    gender_label.append(key)
                    gender_slice.append(gender_dict[key])
                gender_slice = [100 * percentage for percentage in gender_slice]
                explode = []
                for i in range(len(gender_slice)):
                    if gender_slice[i] == max(gender_slice):
                        explode.append(0.1)
                    else:
                        explode.append(0)
                plt.subplot(413)
                plt.pie(gender_slice, labels=gender_label, explode=explode, startangle=90, autopct='%1.2f%%',
                        wedgeprops={'edgecolor': 'black'}, textprops={'fontsize': 7})
                plt.title('Ratio of Gender', fontsize=10)



            life_status_dict = {}
            if 'life_status' in byList:
                total_deathday = ActorsDB.query.filter(ActorsDB.birthday != None).all()
                real_deathday = ActorsDB.query.filter(ActorsDB.birthday != None, ActorsDB.deathday == None).all()
                life_status_dict['alive'] = round(len(real_deathday)/len(total_deathday),2)
                life_status_dict['dead'] = round((len(total_deathday)-len(real_deathday))/len(total_deathday),2)
                json['by-life_status'] = life_status_dict
                life_status_slice = []
                life_status_label = []
                for key in life_status_dict.keys():
                    life_status_label.append(key)
                    life_status_slice.append(life_status_dict[key])
                life_status_slice = [100 * percentage for percentage in life_status_slice]
                explode = []
                for i in range(len(life_status_slice)):
                    if life_status_slice[i] == max(life_status_slice):
                        explode.append(0.1)
                    else:
                        explode.append(0)
                plt.subplot(414)
                plt.pie(life_status_slice, labels=life_status_label, explode=explode, startangle=90, autopct='%1.2f%%',
                        wedgeprops={'edgecolor': 'black'}, textprops={'fontsize': 7})
                plt.title('Ratio of life_status', fontsize=10)
            plt.savefig('image.png')


            if format == 'json':
                return json,200
            elif format == 'image':
                return make_response(send_file('image.png'), 200)
            else:
                return 'This format is illegal!', 403

if __name__ == "__main__":
    db.create_all()
    TVMaze.run(debug=True, port=5000, host='127.0.0.1')